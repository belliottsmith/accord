package accord.coordinate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import accord.api.Key;
import accord.coordinate.tracking.QuorumTracker;
import accord.txn.Ballot;
import accord.messages.Callback;
import accord.local.Node;
import accord.local.Node.Id;
import accord.topology.Shards;
import accord.txn.Timestamp;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.messages.Accept;
import accord.messages.Accept.AcceptOk;
import accord.messages.Accept.AcceptReply;

class Propose extends CompletableFuture<Agreed>
{
    final Node node;
    final Ballot ballot;
    final TxnId txnId;
    final Txn txn;
    final Key homeKey;
    final Shards shards; // TODO: remove, hide in participants

    private List<AcceptOk> acceptOks;
    private Timestamp proposed;
    private QuorumTracker acceptTracker;

    Propose(Node node, Ballot ballot, TxnId txnId, Txn txn, Key homeKey, Shards shards)
    {
        this.node = node;
        this.ballot = ballot;
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.shards = shards;
    }

    protected void startAccept(Timestamp executeAt, Dependencies deps)
    {
        this.proposed = executeAt;
        this.acceptOks = new ArrayList<>();
        this.acceptTracker = new QuorumTracker(shards);
        node.send(acceptTracker.nodes(), new Accept(ballot, txnId, txn, homeKey, executeAt, deps), new Callback<AcceptReply>()
        {
            @Override
            public void onSuccess(Id from, AcceptReply response)
            {
                onAccept(from, response);
            }

            @Override
            public void onFailure(Id from, Throwable throwable)
            {
                acceptTracker.recordFailure(from);
                if (acceptTracker.hasFailed())
                    completeExceptionally(new Timeout());
            }
        });
    }

    private void onAccept(Id from, AcceptReply reply)
    {
        if (isDone())
            return;

        if (!reply.isOK())
        {
            completeExceptionally(new Preempted());
            return;
        }

        AcceptOk ok = (AcceptOk) reply;
        acceptOks.add(ok);
        acceptTracker.recordSuccess(from);

        if (acceptTracker.hasReachedQuorum())
            onAccepted();
    }

    private void onAccepted()
    {
        Dependencies deps = new Dependencies();
        for (AcceptOk acceptOk : acceptOks)
            deps.addAll(acceptOk.deps);
        agreed(proposed, deps);
    }

    protected void agreed(Timestamp executeAt, Dependencies deps)
    {
        complete(new Agreed(txnId, txn, homeKey, executeAt, deps, shards, null, null));
    }
}
