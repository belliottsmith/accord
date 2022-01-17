package accord.messages;

import accord.api.Key;
import accord.local.Node;
import accord.local.Node.Id;
import accord.txn.Timestamp;
import accord.txn.Dependencies;
import accord.txn.Txn;
import accord.txn.TxnId;

// TODO: CommitOk responses, so we can send again if no reply received? Or leave to recovery?
public class Commit extends ReadData implements Request
{
    final Timestamp executeAt;
    final Dependencies deps;
    final boolean read;

    public Commit(TxnId txnId, Txn txn, Key homeKey, Timestamp executeAt, Dependencies deps, boolean read)
    {
        super(txnId, txn, homeKey);
        this.executeAt = executeAt;
        this.deps = deps;
        this.read = read;
    }

    public void process(Node node, Id from, long messageId)
    {
        txn.local(node).forEach(instance -> instance.command(txnId).commit(txn, homeKey, deps, executeAt));
        if (read) super.process(node, from, messageId);
    }

    @Override
    public String toString()
    {
        return "Commit{" +
               "executeAt: " + executeAt +
               ", deps: " + deps +
               ", read: " + read +
               '}';
    }
}
