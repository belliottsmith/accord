package accord.coordinate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import accord.api.Key;
import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.messages.InformOfTxn;
import accord.messages.InformOfTxn.InformOfTxnReply;
import accord.topology.Topology;
import accord.txn.Txn;
import accord.txn.TxnId;

public class InformHomeOfTxn extends CompletableFuture<Void> implements Callback<InformOfTxnReply>
{
    final TxnId txnId;
    final Key homeKey;
    final QuorumShardTracker tracker;
    Throwable failure;

    InformHomeOfTxn(TxnId txnId, Key homeKey, Topology topology)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.tracker = new QuorumShardTracker(topology.forKey(homeKey));
    }

    public static CompletionStage<Void> inform(Node node, TxnId txnId, Txn txn, Key homeKey)
    {
        // TODO: we should not need to send the Txn here, but to avoid that we need to support no-ops
        // TODO (now): consider range of topologies to use
        InformHomeOfTxn inform = new InformHomeOfTxn(txnId, homeKey, node.topology().current());
        node.send(node.topology().current().forKey(homeKey), new InformOfTxn(txnId, homeKey, txn), inform);
        return inform;
    }

    @Override
    public void onSuccess(Id from, InformOfTxnReply response)
    {
        if (response.isOk())
        {
            if (tracker.onSuccess(from) && tracker.hasReachedQuorum())
                complete(null);
        }
        else
        {
            onFailure(from, new StaleTopology());
        }
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        if (failure == null) failure = throwable;
        else failure.addSuppressed(throwable);

        // TODO: if we fail and have an incorrect topology, trigger refresh
        if (tracker.onFailure(from) && tracker.hasFailed())
            completeExceptionally(failure);
    }
}
