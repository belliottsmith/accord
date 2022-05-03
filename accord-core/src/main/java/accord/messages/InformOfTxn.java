package accord.messages;

import accord.api.Key;
import accord.local.Node;
import accord.local.Node.Id;
import accord.txn.Txn;
import accord.txn.TxnId;

import static accord.messages.InformOfTxn.InformOfTxnNack.nack;
import static accord.messages.InformOfTxn.InformOfTxnOk.ok;

public class InformOfTxn implements Request
{
    final TxnId txnId;
    final Key homeKey;
    final Txn txn;

    public InformOfTxn(TxnId txnId, Key homeKey, Txn txn)
    {
        this.txnId = txnId;
        this.homeKey = homeKey;
        this.txn = txn;
    }

    public void process(Node node, Id replyToNode, ReplyContext replyContext)
    {
        Reply reply = node.ifLocal(homeKey, txnId.epoch, instance -> {
            instance.command(txnId).preaccept(txn, homeKey);
            return ok();
        });

        if (reply == null)
            reply = nack();

        node.reply(replyToNode, replyContext, reply);
    }

    @Override
    public String toString()
    {
        return "InformOfTxn{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               '}';
    }

    public interface InformOfTxnReply extends Reply
    {
        boolean isOk();
    }

    public static class InformOfTxnOk implements InformOfTxnReply
    {
        private static final InformOfTxnOk instance = new InformOfTxnOk();

        @Override
        public MessageType type()
        {
            return MessageType.INFORM_RSP;
        }

        static InformOfTxnReply ok()
        {
            return instance;
        }

        private InformOfTxnOk() { }

        @Override
        public boolean isOk()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return "InformOfTxnOk";
        }
    }

    public static class InformOfTxnNack implements InformOfTxnReply
    {
        private static final InformOfTxnNack instance = new InformOfTxnNack();

        @Override
        public MessageType type()
        {
            return MessageType.INFORM_RSP;
        }

        static InformOfTxnReply nack()
        {
            return instance;
        }

        private InformOfTxnNack() { }

        @Override
        public boolean isOk()
        {
            return false;
        }

        @Override
        public String toString()
        {
            return "InformOfTxnNack";
        }
    }

    @Override
    public MessageType type()
    {
        return MessageType.INFORM_REQ;
    }
}