package accord.coordinate;

import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.local.Node;
import accord.local.Status;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.topology.Shard;
import accord.txn.Ballot;
import accord.txn.Txn;
import accord.txn.TxnId;

import static accord.local.Status.Accepted;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class CheckShardProgress extends CheckShardStatus
{
    final Status knownStatus;
    final Ballot knownPromised;
    final boolean knownPromiseHasBeenAccepted;

    CheckShardProgress(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard,
                       Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted, byte includeInfo)
    {
        super(node, txnId, txn, homeKey, homeShard, includeInfo);
        this.knownStatus = knownStatus;
        this.knownPromised = knownPromised;
        this.knownPromiseHasBeenAccepted = knownPromiseHasBeenAccepted;
    }

    public static CheckShardProgress checkShardProgress(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard,
                                                                    Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted)
    {
        return checkShardProgress(node, txnId, txn, homeKey, homeShard, knownStatus, knownPromised, knownPromiseHasBeenAccepted, (byte)0);
    }

    public static CheckShardProgress checkShardProgress(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard,
                                                                    Status knownStatus, Ballot knownPromised, boolean knownPromiseHasBeenAccepted, byte includeInfo)
    {
        CheckShardProgress checkShardProgress = new CheckShardProgress(node, txnId, txn, homeKey, homeShard, knownStatus, knownPromised, knownPromiseHasBeenAccepted, includeInfo);
        checkShardProgress.start();
        return checkShardProgress;
    }

    @Override
    boolean hasMetSuccessCriteria()
    {
        return tracker.hasReachedQuorum() || hasMadeProgress();
    }

    public boolean hasMadeProgress()
    {
        return max != null && (
                  max.isCoordinating
               || max.status.compareTo(knownStatus) > 0
               || max.promised.compareTo(knownPromised) > 0
               || (!knownPromiseHasBeenAccepted && knownStatus == Accepted && max.accepted.equals(knownPromised)));
    }

    @Override
    void onSuccessCriteriaOrExhaustion()
    {
        complete(max);
    }
}
