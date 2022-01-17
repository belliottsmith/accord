package accord.api;

import accord.coordinate.CheckOnUncommitted;
import accord.coordinate.InformHomeOfTxn;
import accord.local.CommandStore;
import accord.txn.TxnId;

/**
 * This interface is responsible for managing incomplete transactions *and retrying them*.
 * Each stage is fenced by two methods, one entry and one exit. The entry method notifies the implementation
 * that it should soon be notified of the exit method, and if it is not that it should invoke some
 * pre-specified recovery mechanism.
 *
 * This is a per-CommandStore structure, with transactions primarily being managed by their home shard,
 * except during PreAccept as a transaction may not yet have been durably recorded by the home shard.
 *
 * The basic logical flow for ensuring a transaction is committed and applied at all replicas is as follows:
 *
 *  - First, ensure a quorum of the home shard is aware of the transaction by invoking {@link InformHomeOfTxn}.
 *    Entry by {@link #nonHomePreaccept} Non-home shards may now forget this transaction for replay purposes
 *    ({@link #nonHomePostPreaccept}).
 *
 *  - Non-home shards may also be informed of transactions that are blocking the progress of other transactions.
 *    If the {@code waitingOn} transaction that is blocking progress is uncommitted it is required that the progress
 *    log invoke {@link CheckOnUncommitted} for the transaction if no {@link #nonHomeCommit(TxnId)} is witnessed.
 *
 *  - Members of the home shard will be informed of a transaction to monitor by the invocation of {@link #uncommitted}.
 *    If this is not followed closely by {@link #committed}, {@link accord.coordinate.MaybeRecover} should be invoked.
 *
 *  - Members of the home shard will later be informed that the transaction is {@link #readyToExecute}.
 *    If this is not followed closely by {@link #executed(TxnId)}, {@link accord.coordinate.MaybeRecover} should be invoked.
 *
 *  - Finally, it is up to each shard to independently coordinate disseminating the write to every replica.
 *
 * Alternatively this may be viewed as a state machine with the following states:
 *
 *         NonHomePreAccept, NonHomeSafe, NonHomeWaitingOn,
 *         Uncommitted, Committed,
 *         ReadyToExecute, Executed,
 *         WaitingForExecutedOnAllShards, ExecutedOnAllShards
 */
public interface ProgressLog
{
    interface Factory
    {
        ProgressLog create(CommandStore store);
    }

    /**
     * Has been witnessed as uncommitted
     */
    void uncommitted(TxnId txnId);

    /**
     * Has committed
     */
    void committed(TxnId txnId);

    /**
     * The home shard is now waiting to make progress, as all local dependencies have applied
     */
    void readyToExecute(TxnId txnId);

    /**
     * The transaction's outcome has been durably recorded (but not necessarily applied) locally.
     * It will be applied once all local dependencies have been.
     *
     * Invoked on both home and non-home command stores, and is required to trigger per-shard processes
     * that ensure the transaction's outcome is durably persisted on all replicas of the shard.
     *
     * May also permit aborting a pending waitingOn-triggered event.
     */
    void executed(TxnId txnId);

    /**
     * The transaction's outcome has been durably recorded (but not necessarily applied) at a quorum of all shards.
     */
    void executedOnAllShards(TxnId txnId);

    /**
     * Has been witnessed as pre-accepted by a non-home command store, so it will be tracked only long enough
     * to be certain it is known by the home shard.
     */
    void nonHomePreaccept(TxnId txnId);

    /**
     * Has been witnessed as accepted or later by a non-home command store, so it may stop tracking the transaction.
     */
    void nonHomePostPreaccept(TxnId txnId);

    /**
     * Has been witnessed as committed by a non-home command store. This may permit cancellation of a
     * pending waitingOn operation.
     */
    void nonHomeCommit(TxnId txnId);

    /**
     * The parameter is a command that some other command's execution is most proximally blocked by.
     * This may be invoked by either the home or non-home command store.
     *
     * If invoked by the non-home command store on a transaction that has not yet been committed, this must
     * eventually trigger contact with the home shard in order to check on the transaction's progress
     * (unless the transaction is committed first). This is to avoid unnecessary additional messages being exchanged
     * in the common case, where a transaction may be committed successfully to members of its home shard, but not
     * to all non-home shards. In such a case the transaction may be a false-dependency of another transaction that
     * needs to perform a read, and all nodes which may do so are waiting for the commit record to arrive.
     *
     * In all other scenarios, the implementation is free to choose its course of action.
     *
     * TODO (aborts): waitingOnTxn should not be a parameter; only known locally involved keys (not necessarily all keys)
     */
    void waitingOn(TxnId waiting, TxnId waitingOn);
}
