package accord.messages;

import accord.api.Key;
import accord.local.Node;
import accord.local.Node.Id;
import accord.txn.TxnId;

import static accord.messages.InformOfTxn.InformOfTxnNack.nack;
import static accord.messages.InformOfTxn.InformOfTxnOk.ok;

public class InformOfPersistence implements Request
{
    final TxnId txnId;
    final Key homeKey;

    public InformOfPersistence(TxnId txnId, Key homeKey)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
    }

    public void process(Node node, Id replyToNode, long replyToMessage)
    {
        node.reply(replyToNode, replyToMessage, node.local(homeKey).map(instance -> {
            instance.command(txnId).setGloballyPersistent();
            instance.progressLog().executedOnAllShards(txnId);
            return ok();
        }).orElse(nack()));
    }

    @Override
    public String toString()
    {
        return "InformOfPersistence{" +
               "txnId:" + txnId +
               '}';
    }
}
