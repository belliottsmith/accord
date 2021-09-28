package accord.txn;

import accord.api.Write;
import accord.local.CommandShard;

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

    public void apply(CommandShard commands)
    {
        if (write != null)
            write.apply(commands.ranges(), executeAt, commands.store());
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
