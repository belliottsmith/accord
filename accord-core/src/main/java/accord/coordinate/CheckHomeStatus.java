package accord.coordinate;

import java.util.concurrent.CompletionStage;

import accord.api.Key;
import accord.local.Node;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.topology.Shard;
import accord.txn.Txn;
import accord.txn.TxnId;

/**
 * A result of null indicates the transaction is globally persistent
 * A result of CheckStatusOk indicates the maximum status found for the transaction, which may be used to assess progress
 */
public class CheckHomeStatus extends CheckShardStatus
{
    CheckHomeStatus(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, byte includeInfo)
    {
        super(node, txnId, txn, homeKey, homeShard, includeInfo);
    }

    public static CompletionStage<CheckStatusOk> checkHomeStatus(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard)
    {
        return checkHomeStatus(node, txnId, txn, homeKey, homeShard, (byte)0);
    }

    public static CompletionStage<CheckStatusOk> checkHomeStatus(Node node, TxnId txnId, Txn txn, Key homeKey, Shard homeShard, byte includeInfo)
    {
        CheckHomeStatus checkHomeStatus = new CheckHomeStatus(node, txnId, txn, homeKey, homeShard, includeInfo);
        checkHomeStatus.start();
        return checkHomeStatus;
    }

    @Override
    boolean hasMetSuccessCriteria()
    {
        return tracker.hasReachedQuorum();
    }

    void onSuccessCriteriaOrExhaustion()
    {
        complete(max);
    }
}
