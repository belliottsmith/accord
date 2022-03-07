package accord.messages;

import accord.api.Key;
import accord.local.Node;
import accord.local.Node.Id;
import accord.api.Result;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Writes;
import accord.txn.Txn;
import accord.txn.TxnId;

public class Apply implements Request
{
    protected final TxnId txnId;
    protected final Txn txn;
    protected final Key homeKey;
    protected final Timestamp executeAt;
    // TODO: these only need to be sent if we don't know if this node has witnessed a Commit
    protected final Dependencies deps;
    protected final Writes writes;
    protected final Result result;

    public Apply(TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Dependencies deps, Writes writes, Result result)
    {
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
        this.deps = deps;
        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
    }

    public void process(Node node, Id replyToNode, long replyToMessage)
    {
        txn.local(node).forEach(instance -> instance.command(txnId).apply(txn, homeKey, deps, executeAt, writes, result));
        node.reply(replyToNode, replyToMessage, ApplyOk.INSTANCE);
    }

    public static class ApplyOk implements Reply
    {
        public static final ApplyOk INSTANCE = new ApplyOk();
        public ApplyOk() {}

        @Override
        public String toString()
        {
            return "ApplyOk";
        }
    }

    @Override
    public String toString()
    {
        return "Apply{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               ", deps:" + deps +
               ", executeAt:" + executeAt +
               ", writes:" + writes +
               ", result:" + result +
               '}';
    }
}
