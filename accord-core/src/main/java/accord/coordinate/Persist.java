package accord.coordinate;

import accord.api.Key;
import accord.api.Result;
import accord.coordinate.tracking.QuorumTracker;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Apply;
import accord.messages.Apply.ApplyOk;
import accord.messages.Callback;
import accord.messages.InformOfPersistence;
import accord.topology.Topologies;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

// TODO: do not extend AsyncFuture, just use a simple BiConsumer callback
public class Persist extends AsyncFuture<Void> implements Callback<ApplyOk>
{
    final Node node;
    final TxnId txnId;
    final Key homeKey;
    final QuorumTracker tracker;
    Throwable failure;

    public static AsyncFuture<Void> persist(Node node, Topologies topologies, TxnId txnId, Key homeKey, Txn txn, Timestamp executeAt, Dependencies deps, Writes writes, Result result)
    {
        Persist persist = new Persist(node, topologies, txnId, homeKey);
        node.send(topologies.nodes(), to -> new Apply(to, topologies, txnId, txn, homeKey, executeAt, deps, writes, result), persist);
        return persist;
    }

    private Persist(Node node, Topologies topologies, TxnId txnId, Key homeKey)
    {
        this.node = node;
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.tracker = new QuorumTracker(topologies);
    }

    @Override
    public void onSuccess(Id from, ApplyOk response)
    {
        if (tracker.success(from) && !isDone())
        {
            // TODO: send to non-home replicas also, so they may clear their log more easily?
            // TODO (now): decide if we send to all home replicas across all epochs, or just original epoch or latest?
            node.send(tracker.topologies().current().forKey(homeKey), new InformOfPersistence(txnId, homeKey));
            trySuccess(null);
        }
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        if (failure == null) failure = throwable;
        else failure.addSuppressed(throwable);

        if (tracker.failure(from))
            tryFailure(failure);
    }
}
