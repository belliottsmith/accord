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
public class CheckOnUncommitted extends CheckOnCommitted
{
    // the maximum execution timestamp of a transaction that has been committed with this as a dependency
    // if we receive a quorum of responses in the home shard that have not witnessed the transaction, then we know
    // it must take a later executeAt
    @Nullable final Timestamp maxExecuteAtWithTxnAsDependency;

    CheckOnUncommitted(Node node, TxnId txnId, Txn txn, Key someKey, Shard someShard, long shardEpoch,
                       @Nullable Timestamp maxExecuteAtWithTxnAsDependency)
    {
        super(node, txnId, txn, someKey, someShard, shardEpoch);
        this.maxExecuteAtWithTxnAsDependency = maxExecuteAtWithTxnAsDependency;
    }

    public static CheckOnUncommitted checkOnUncommitted(Node node, TxnId txnId, Txn txn, Key someKey, Shard someShard, long shardEpoch, Timestamp maxExecuteAtWithTxnAsDependency)
    {
        CheckOnUncommitted checkOnUncommitted = new CheckOnUncommitted(node, txnId, txn, someKey, someShard, shardEpoch, maxExecuteAtWithTxnAsDependency);
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
    void onSuccessCriteriaOrExhaustion(CheckStatusOkFull full)
    {
        switch (full.status)
        {
            default: throw new IllegalStateException();
            case NotWitnessed:
                if (maxExecuteAtWithTxnAsDependency != null)
                {
                    node.forEachLocalSince(txn.keys, txnId.epoch, commandStore -> {
                        Command command = commandStore.ifPresent(txnId);
                        if (command != null)
                            command.mustExecuteAfter(maxExecuteAtWithTxnAsDependency);
                    });
                }
                break;
            case PreAccepted:
            case Accepted:
                node.forEachLocalSince(txn.keys, txnId.epoch, commandStore -> {
                    Command command = commandStore.ifPresent(txnId);
                    if (command != null)
                        command.homeKey(full.homeKey);
                });
                break;
            case Executed:
            case Applied:
            case Committed:
            case ReadyToExecute:
                super.onSuccessCriteriaOrExhaustion(full);
        }
    }
}
