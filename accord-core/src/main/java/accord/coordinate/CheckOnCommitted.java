package accord.coordinate;

import accord.api.Key;
import accord.local.Command;
import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.topology.Shard;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;

import static accord.local.Status.Executed;

/**
 * Check on the status of a locally-uncommitted transaction. Returns early if any result indicates Committed, otherwise
 * waits only for a quorum and returns the maximum result.
 *
 * Updates local command stores based on the obtained information.
 */
public class CheckOnCommitted extends CheckShardStatus
{
    final Timestamp blockedAt;
    CheckOnCommitted(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, long homeEpoch, Timestamp blockedAt)
    {
        super(node, txnId, txn, homeKey, homeShard, homeEpoch, IncludeInfo.all());
        this.blockedAt = blockedAt;
    }

    public static CheckOnCommitted checkOnCommitted(Node node, TxnId txnId, Txn txn, Key someKey, Shard someShard, long shardEpoch, Timestamp blockedAt)
    {
        CheckOnCommitted checkOnCommitted = new CheckOnCommitted(node, txnId, txn, someKey, someShard, shardEpoch, blockedAt);
        checkOnCommitted.start();
        return checkOnCommitted;
    }

    @Override
    boolean hasMetSuccessCriteria()
    {
        return tracker.hasReachedQuorum() || hasApplied();
    }

    public boolean hasApplied()
    {
        return max != null && max.status.compareTo(Executed) >= 0;
    }

    @Override
    void onSuccessCriteriaOrExhaustion()
    {
        try
        {
            CheckStatusOkFull full = (CheckStatusOkFull) max;
            Key localKey = node.trySelectLocalKey(txnId.epoch, txn.keys, full.homeKey);
            switch (full.status)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                    // TODO report exception? should have seen Committed at least
                    break;
                case Executed:
                case Applied:
                    node.forEachLocal(txn.keys, full.executeAt.epoch, blockedAt.epoch, commandStore -> {
                        Command command = commandStore.command(txnId);
                        command.apply(txn, full.homeKey, localKey, full.executeAt, full.deps, full.writes, full.result);
                    });
                case Committed:
                case ReadyToExecute:
                    node.forEachLocal(txn.keys, txnId.epoch, blockedAt.epoch, commandStore -> {
                        Command command = commandStore.command(txnId);
                        command.commit(txn, full.homeKey, localKey, full.executeAt, full.deps);
                    });
            }
        }
        catch (Throwable t)
        {
            complete(max);
            throw t;
        }
        complete(max);
    }
}
