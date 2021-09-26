package accord.local;

import accord.api.Key;
import accord.api.KeyRange;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.txn.Keys;
import accord.txn.TxnId;
import com.google.common.base.Preconditions;

import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Single threaded subdivision of accord metadata
 */
public class CommandShard
{
    private static class SubRanges extends KeyRanges
    {
        private static final SubRanges NONE = new SubRanges(new KeyRange[0], new Shard[0]);

        private final Shard[] shards;

        public SubRanges(KeyRange[] ranges, Shard[] shards)
        {
            super(ranges);
            Preconditions.checkArgument(ranges.length == shards.length);
            for (int i=0; i<ranges.length; i++)
                Preconditions.checkArgument(shards[i].range.fullyContains(ranges[i]));
            this.shards = shards;
        }
    }

    private SubRanges subRanges = SubRanges.NONE;

    private final NavigableMap<TxnId, Command> commands = new TreeMap<>();
    private final NavigableMap<Key, CommandsForKey> commandsForKey = new TreeMap<>();

    public boolean intersects(Keys keys)
    {
        return subRanges.intersects(keys);
    }
}
