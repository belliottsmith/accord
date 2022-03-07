package accord.txn;

import java.util.stream.Stream;

import accord.api.*;
import accord.local.*;
import accord.topology.KeyRanges;

public class Txn
{
    enum Kind { READ, WRITE, RECONFIGURE }

    final Kind kind;
    public final Keys keys;
    public final Read read;
    public final Query query;
    public final Update update;

    public Txn(Keys keys, Read read, Query query)
    {
        this.kind = Kind.READ;
        this.keys = keys;
        this.read = read;
        this.query = query;
        this.update = null;
    }

    public Txn(Keys keys, Read read, Query query, Update update)
    {
        this.kind = Kind.WRITE;
        this.keys = keys;
        this.read = read;
        this.update = update;
        this.query = query;
    }

    public boolean isWrite()
    {
        switch (kind)
        {
            default:
                throw new IllegalStateException();
            case READ:
                return false;
            case WRITE:
            case RECONFIGURE:
                return true;
        }
    }

    public Result result(Data data)
    {
        return query.compute(data);
    }

    public Writes execute(Timestamp executeAt, Data data)
    {
        if (update == null)
            return new Writes(executeAt, keys, null);

        return new Writes(executeAt, keys, update.apply(data));
    }

    public Keys keys()
    {
        return keys;
    }

    public String toString()
    {
        return "{read:" + read.toString() + (update != null ? ", update:" + update : "") + '}';
    }

    public Data read(KeyRanges range, DataStore store)
    {
        return read.read(range, store);
    }

    public Data read(Command command)
    {
        CommandStore commandStore = command.commandStore;
        return read(commandStore.ranges(), commandStore.store());
    }

    // TODO: move these somewhere else?
    public Stream<CommandStore> local(Node node)
    {
        return node.local(keys());
    }
}
