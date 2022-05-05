package accord.coordinate;

import javax.annotation.Nullable;

import accord.api.Key;
import accord.local.Command;
import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.topology.Shard;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;

import static accord.local.Status.Committed;
import static accord.messages.CheckStatus.IncludeInfo.Dependencies;
import static accord.messages.CheckStatus.IncludeInfo.ExecuteAt;
import static accord.messages.CheckStatus.IncludeInfo.HomeKey;

/**
 * Check on the status of a locally-uncommitted transaction. Returns early if any result indicates Committed, otherwise
 * waits only for a quorum and returns the maximum result.
 *
 * Updates local command stores based on the obtained information.
 */
public class CheckOnUncommitted extends CheckShardStatus
{
    // the maximum execution timestamp of a transaction that has been committed with this as a dependency
    // if we receive a quorum of responses in the home shard that have not witnessed the transaction, then we know
    // it must take a later executeAt
    @Nullable final Timestamp maxExecuteAtWithTxnAsDependency;

    CheckOnUncommitted(Node node, TxnId txnId, Txn txn, Key someKey, Shard someShard, long shardEpoch,
                       @Nullable Timestamp maxExecuteAtWithTxnAsDependency, byte includeInfo)
    {
        super(node, txnId, txn, someKey, someShard, shardEpoch, includeInfo);
        this.maxExecuteAtWithTxnAsDependency = maxExecuteAtWithTxnAsDependency;
    }

    public static CheckOnUncommitted checkOnUncommitted(Node node, TxnId txnId, Txn txn, Key someKey, Shard someShard, long shardEpoch, Timestamp maxExecuteAtWithTxnAsDependency)
    {
        return checkOnUncommitted(node, txnId, txn, someKey, someShard, shardEpoch, maxExecuteAtWithTxnAsDependency, (byte)0);
    }

    public static CheckOnUncommitted checkOnUncommitted(Node node, TxnId txnId, Txn txn, Key someKey, Shard someShard, long shardEpoch, Timestamp maxExecuteAtWithTxnAsDependency, byte includeInfo)
    {
        CheckOnUncommitted checkOnUncommitted = new CheckOnUncommitted(node, txnId, txn, someKey, someShard, shardEpoch, maxExecuteAtWithTxnAsDependency, (byte) (includeInfo | HomeKey.and(Dependencies.and(ExecuteAt))));
        checkOnUncommitted.start();
        return checkOnUncommitted;
    }

    @Override
    boolean hasMetSuccessCriteria()
    {
        return tracker.hasReachedQuorum() || hasCommitted();
    }

    public boolean hasCommitted()
    {
        return max != null && max.status.compareTo(Committed) >= 0;
    }

    @Override
    void onSuccessCriteriaOrExhaustion()
    {
        try
        {
            CheckStatusOkFull full = (CheckStatusOkFull) max;
            long minEpoch = txnId.epoch;
            long maxEpoch = (maxExecuteAtWithTxnAsDependency == null ? txnId : maxExecuteAtWithTxnAsDependency).epoch;
            node.forEachLocal(txn.keys, minEpoch, maxEpoch, commandStore -> {
                Command command = commandStore.command(txnId);
                switch (full.status)
                {
                    default: throw new AssertionError();
                    case NotWitnessed:
                        // TODO (now): the home shard might be out of date here and might not have been contacted
                        //             since we don't guarantee talking to the earlier shards. either make sure we contact
                        //             them, or else
                        // if not witnessed by a quorum of the home shard
                        if (maxExecuteAtWithTxnAsDependency != null)
                            command.mustExecuteAfter(maxExecuteAtWithTxnAsDependency);
                        break;
                    case PreAccepted:
                    case Accepted:
                        command.homeKey(full.homeKey);
                        break;
                    case Executed:
                    case Applied:
                        // TODO (now): should we merge with CheckOnCommitted and just apply here if the local node accepts it?
                    case Committed:
                    case ReadyToExecute:
                        Key progressKey = node.trySelectProgressKey(txnId, txn.keys, full.homeKey);
                        command.commit(txn, full.homeKey, progressKey, full.executeAt, full.deps);
                        break;
                }
            });
        }
        catch (Throwable t)
        {
            complete(max);
            throw t;
        }
        complete(max);
    }
}
