package accord.coordinate;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import accord.api.Data;
import accord.api.Key;
import accord.coordinate.tracking.ReadTracker;
import accord.api.Result;
import accord.messages.Callback;
import accord.local.Node;
import accord.txn.Dependencies;
import accord.messages.ReadData.ReadReply;
import accord.topology.Shards;
import accord.local.Node.Id;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Keys;
import accord.messages.Commit;
import accord.messages.ReadData;
import accord.messages.ReadData.ReadOk;
import com.google.common.base.Preconditions;

class Execute extends CompletableFuture<Result> implements Callback<ReadReply>
{
    final Node node;
    final TxnId txnId;
    final Txn txn;
    final Key homeKey;
    final Timestamp executeAt;
    final Shards shards;
    final Keys keys;
    final Dependencies deps;
    final ReadTracker tracker;
    private Data data;
    final int replicaIndex;

    private Execute(Node node, Agreed agreed)
    {
        this.node = node;
        this.txnId = agreed.txnId;
        this.txn = agreed.txn;
        this.homeKey = agreed.homeKey;
        this.keys = txn.keys();
        this.deps = agreed.deps;
        this.executeAt = agreed.executeAt;
        this.shards = agreed.shards;
        this.tracker = new ReadTracker(shards);
        this.replicaIndex = node.random().nextInt(shards.get(0).nodes.size());

        // TODO: perhaps compose these different behaviours differently?
        if (agreed.applied != null)
        {
            complete(agreed.result);
            Persist.persist(node, shards, txnId, homeKey, txn, executeAt, deps, agreed.applied, agreed.result);
        }
        else
        {
            Set<Id> readSet = tracker.computeMinimalReadSetAndMarkInflight();
            for (Node.Id to : tracker.nodes())
            {
                boolean read = readSet.contains(to);
                Commit send = new Commit(txnId, txn, homeKey, executeAt, agreed.deps, read);
                if (read)
                {
                    node.send(to, send, this);
                }
                else
                {
                    node.send(to, send);
                }
            }
        }
    }

    @Override
    public void onSuccess(Id from, ReadReply reply)
    {
        if (isDone())
            return;

        if (!reply.isFinal())
            return;

        if (!reply.isOK())
        {
            completeExceptionally(new Preempted());
            return;
        }

        data = data == null ? ((ReadOk) reply).data
                            : data.merge(((ReadOk) reply).data);

        tracker.recordReadSuccess(from);

        if (tracker.hasCompletedRead())
        {
            Result result = txn.result(data);
            complete(result);
            Persist.persist(node, shards, txnId, homeKey, txn, executeAt, deps, txn.execute(executeAt, data), result);
        }
    }

    @Override
    public void onFailure(Id from, Throwable throwable)
    {
        // try again with another random node
        // TODO: API hooks
        if (!(throwable instanceof Timeout))
            throwable.printStackTrace();

        // TODO: introduce two tiers of timeout, one to trigger a retry, and another to mark the original as failed
        // TODO: if we fail, nominate another coordinator from the homeKey shard to try
        tracker.recordReadFailure(from);
        Set<Id> readFrom = tracker.computeMinimalReadSetAndMarkInflight();
        if (readFrom == null)
        {
            Preconditions.checkState(tracker.hasFailed());
            completeExceptionally(throwable);
        }
        else
            node.send(readFrom, new ReadData(txnId, txn, homeKey), this);
    }

    static CompletionStage<Result> execute(Node instance, Agreed agreed)
    {
        return new Execute(instance, agreed);
    }
}
