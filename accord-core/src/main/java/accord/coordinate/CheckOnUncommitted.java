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
    @Nullable final Timestamp maxWaiting; // the maximum execution timestamp of a transaction that has been committed with this as a dependency

    CheckOnUncommitted(Node node, TxnId txnId, Txn txn, Key homeKey, @Nullable Timestamp maxWaiting, Shard homeShard, byte includeInfo)
    {
        super(node, txnId, txn, homeKey, homeShard, includeInfo);
        this.maxWaiting = maxWaiting;
    }

    public static CheckOnUncommitted checkOnUncommitted(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, Timestamp maxWaiting)
    {
        return checkOnUncommitted(node, txnId, txn, homeKey, homeShard, maxWaiting, (byte)0);
    }

    public static CheckOnUncommitted checkOnUncommitted(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, Timestamp maxWaiting, byte includeInfo)
    {
        CheckOnUncommitted checkOnUncommitted = new CheckOnUncommitted(node, txnId, txn, homeKey, maxWaiting, homeShard, (byte) (includeInfo | HomeKey.and(Dependencies.and(ExecuteAt))));
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
            node.local(txn.keys).forEach(commandStore -> {
                Command command = commandStore.command(txnId);
                switch (full.status)
                {
                    default: throw new AssertionError();
                    case NotWitnessed:
                        if (maxWaiting != null)
                            command.mustExecuteAfter(maxWaiting);
                        break;
                    case PreAccepted:
                    case Accepted:
                        command.homeKey(full.homeKey);
                        break;
                    case Committed:
                    case ReadyToExecute:
                    case Executed:
                    case Applied:
                        command.commit(txn, full.homeKey, full.deps, full.executeAt);
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
