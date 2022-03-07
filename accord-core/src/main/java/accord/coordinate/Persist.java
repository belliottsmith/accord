package accord.coordinate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Apply;
import accord.messages.Apply.ApplyOk;
import accord.messages.Callback;
import accord.messages.InformOfPersistence;
import accord.topology.Shards;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;

// TODO: do not extend CompletableFuture, just use a simple BiConsumer callback
public class Persist extends CompletableFuture<Void> implements Callback<ApplyOk>
{
    final Node node;
    final TxnId txnId;
    final Key homeKey;
    final QuorumTracker tracker;
    Throwable failure;

    public static CompletionStage<Void> persist(Node node, Shards shards, TxnId txnId, Key homeKey, Txn txn, Timestamp executeAt, Dependencies deps, Writes writes, Result result)
    {
        Persist persist = new Persist(node, shards, txnId, homeKey);
        node.send(shards, new Apply(txnId, txn, homeKey, executeAt, deps, writes, result), persist);
        return persist;
    }

    private Persist(Node node, Shards shards, TxnId txnId, Key homeKey)
    {
        this.node = node;
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.tracker = new QuorumTracker(shards);
    }

    @Override
    public void onSuccess(Id from, ApplyOk response)
    {
        if (tracker.recordSuccess(from) && tracker.hasReachedQuorum() && !isDone())
        {
            // TODO: send to non-home replicas also, so they may clear their log more easily?
            node.send(node.cluster().forKey(homeKey), new InformOfPersistence(txnId, homeKey));
            complete(null);
        }
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        if (failure == null) failure = throwable;
        else failure.addSuppressed(throwable);

        if (tracker.recordFailure(from) && tracker.hasFailed())
            completeExceptionally(failure);
    }
}
