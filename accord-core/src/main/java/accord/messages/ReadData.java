package accord.messages;

import java.util.Set;
import java.util.stream.Collectors;

import accord.api.Key;
import accord.local.*;
import accord.local.Node.Id;
import accord.api.Data;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.utils.DeterministicIdentitySet;

public class ReadData implements Request
{
    static class LocalRead implements Listener
    {
        final TxnId txnId;
        final Node node;
        final Node.Id replyToNode;
        final long replyToMessage;

        Data data;
        boolean isObsolete; // TODO: respond with the Executed result we have stored?
        Set<CommandStore> waitingOn;

        LocalRead(TxnId txnId, Node node, Id replyToNode, long replyToMessage)
        {
            this.txnId = txnId;
            this.node = node;
            this.replyToNode = replyToNode;
            this.replyToMessage = replyToMessage;
        }

        @Override
        public synchronized void onChange(Command command)
        {
            switch (command.status())
            {
                case NotWitnessed:
                case PreAccepted:
                case Accepted:
                case Committed:
                    return;

                case Executed:
                case Applied:
                    obsolete();
                case ReadyToExecute:
            }

            command.removeListener(this);
            if (!isObsolete)
                read(command);
        }

        private void read(Command command)
        {
            // TODO: threading/futures (don't want to perform expensive reads within this mutually exclusive context)
            Data next = command.txn().read(command);
            data = data == null ? next : data.merge(next);

            waitingOn.remove(command.commandStore);
            if (waitingOn.isEmpty())
                node.reply(replyToNode, replyToMessage, new ReadOk(data));
        }

        void obsolete()
        {
            if (!isObsolete)
            {
                isObsolete = true;
                node.reply(replyToNode, replyToMessage, new ReadNack());
            }
        }

        synchronized void setup(TxnId txnId, Txn txn, Key homeKey)
        {
            waitingOn = txn.local(node).collect(Collectors.toCollection(DeterministicIdentitySet::new));
            // FIXME: fix/check thread safety
            CommandStore.onEach(waitingOn, instance -> {
                Command command = instance.command(txnId);
                command.preaccept(txn, homeKey); // ensure pre-accepted
                switch (command.status())
                {
                    case NotWitnessed:
                        throw new IllegalStateException();
                    case PreAccepted:
                    case Accepted:
                    case Committed:
                        command.addListener(this);
                        break;

                    case Executed:
                    case Applied:
                        obsolete();
                        break;

                    case ReadyToExecute:
                        if (!isObsolete)
                            read(command);
                }
            });
        }
    }

    final TxnId txnId;
    final Txn txn;
    final Key homeKey;

    public ReadData(TxnId txnId, Txn txn, Key homeKey)
    {
        this.txnId = txnId;
        this.txn = txn;
        this.homeKey = homeKey;
    }

    public void process(Node node, Node.Id from, long messageId)
    {
        new LocalRead(txnId, node, from, messageId).setup(txnId, txn, homeKey);
    }

    public static class ReadReply implements Reply
    {
        public boolean isOK()
        {
            return true;
        }
    }

    public static class ReadNack extends ReadReply
    {
        @Override
        public boolean isOK()
        {
            return false;
        }
    }

    public static class ReadOk extends ReadReply
    {
        public final Data data;
        public ReadOk(Data data)
        {
            this.data = data;
        }

        @Override
        public String toString()
        {
            return "ReadOk{" + data + '}';
        }
    }

    @Override
    public String toString()
    {
        return "ReadData{" +
               "txnId:" + txnId +
               ", txn:" + txn +
               '}';
    }
}
