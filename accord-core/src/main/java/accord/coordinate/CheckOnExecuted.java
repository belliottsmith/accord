package accord.coordinate;

import accord.api.Key;
import accord.local.Command;
import accord.local.Node;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.topology.Shard;
import accord.txn.Txn;
import accord.txn.TxnId;

import static accord.local.Status.Executed;

/**
 * Check on the status of a locally-uncommitted transaction. Returns early if any result indicates Committed, otherwise
 * waits only for a quorum and returns the maximum result.
 *
 * Updates local command stores based on the obtained information.
 */
public class CheckOnExecuted extends CheckShardStatus
{
    CheckOnExecuted(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, long homeEpoch)
    {
        super(node, txnId, txn, homeKey, homeShard, homeEpoch, IncludeInfo.all());
    }

    public static CheckOnExecuted checkOnExecuted(Node node, TxnId txnId, Txn txn, Key someKey, Shard someShard, long shardEpoch)
    {
        CheckOnExecuted checkOnExecuted = new CheckOnExecuted(node, txnId, txn, someKey, someShard, shardEpoch);
        checkOnExecuted.start();
        return checkOnExecuted;
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
            switch (full.status)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                    // TODO report exception? should have seen Executed at least
                    break;
                case Executed:
                case Applied:
                    node.forEachLocal(txn.keys, full.executeAt.epoch, full.executeAt.epoch, commandStore -> {
                        Command command = commandStore.command(txnId);
                        command.apply(txn, full.homeKey, full.executeAt, full.deps, full.writes, full.result);
                    });
                case Committed:
                case ReadyToExecute:
                    node.forEachLocal(txn.keys, txnId.epoch, txnId.epoch, commandStore -> {
                        Command command = commandStore.command(txnId);
                        command.commit(txn, full.homeKey, full.executeAt, full.deps);
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
