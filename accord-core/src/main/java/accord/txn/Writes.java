package accord.txn;

import accord.api.Key;
import accord.api.Write;
import accord.local.CommandStore;

public class Writes
{
    public final Timestamp executeAt;
    public final Keys keys;
    public final Write write;

    public Writes(Timestamp executeAt, Keys keys, Write write)
    {
        this.executeAt = executeAt;
        this.keys = keys;
        this.write = write;
    }

    public void apply(CommandStore commandStore)
    {
        if (write == null)
            return;

        keys.accumulate(commandStore.ranges(), new Keys.NonTerminatingKeyAccumulator<Void>() {
            @Override
            public Void accumulate(Key key, Void value)
            {
                if (commandStore.hashIntersects(key))
                    write.apply(key, executeAt, commandStore.store());
                return null;
            }
        });
    }

    @Override
    public String toString()
    {
        return "TxnWrites{" +
               "executeAt:" + executeAt +
               ", keys:" + keys +
               ", write:" + write +
               '}';
    }
}
