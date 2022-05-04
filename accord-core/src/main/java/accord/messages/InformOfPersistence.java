package accord.messages;

import accord.api.Key;
import accord.local.Node;
import accord.local.Node.Id;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.TxnId;

import static accord.messages.InformOfTxn.InformOfTxnNack.nack;
import static accord.messages.InformOfTxn.InformOfTxnOk.ok;

public class InformOfPersistence implements Request
{
    final TxnId txnId;
    final Key homeKey;
    final Timestamp executeAt;

    public InformOfPersistence(TxnId txnId, Key homeKey, Timestamp executeAt)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.executeAt = executeAt;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Reply reply = node.ifLocal(homeKey, txnId.epoch, instance -> {
            instance.command(txnId).setGloballyPersistent(homeKey, executeAt);
            instance.progressLog().executedOnAllShards(txnId);
            return ok();
        });

        if (reply == null)
            reply = nack();

        node.reply(replyToNode, replyContext, reply);
    }

    @Override
    public String toString()
    {
        return "InformOfPersistence{" +
               "txnId:" + txnId +
               '}';
    }

    @Override
    public MessageType type()
    {
        return MessageType.INFORM_PERSISTED_REQ;
    }
}
