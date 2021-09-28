package accord.local;

import accord.api.Key;
import accord.api.KeyRange;
import accord.topology.KeyRanges;
import accord.topology.Shards;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.TxnId;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Single threaded subdivision of accord metadata
 */
public abstract class CommandShard
{
    private final int index;

    private static class RangeMapping
    {
        private static final RangeMapping EMPTY = new RangeMapping(KeyRanges.EMPTY, Shards.EMPTY);
        private final KeyRanges ranges;
        private final Topology topology;

        public RangeMapping(KeyRanges ranges, Topology topology)
        {
            this.ranges = ranges;
            this.topology = topology;
        }
    }

    public CommandShard(int index)
    {
        this.index = index;
    }

    private volatile RangeMapping rangeMap = RangeMapping.EMPTY;


    private final NavigableMap<TxnId, Command> commands = new TreeMap<>();
    private final NavigableMap<Key, CommandsForKey> commandsForKey = new TreeMap<>();

    void updateTopology(Topology topology, KeyRanges added, KeyRanges removed)
    {
        KeyRanges newRanges = rangeMap.ranges.difference(removed).add(added).mergeTouching();
        rangeMap = new RangeMapping(newRanges, topology);
        // TODO: map ranges to shards

        for (KeyRange range : removed)
        {
            NavigableMap<Key, CommandsForKey> subMap = commandsForKey.subMap(range.start(), range.startInclusive(), range.end(), range.endInclusive());
            for (Key key : subMap.keySet())
            {
                CommandsForKey forKey = commandsForKey.remove(key);
                if (forKey == null)
                    continue;

                for (Command command : forKey)
                    if (command.txn() != null && !rangeMap.ranges.intersects(command.txn().keys))
                        commands.remove(command.txnId());
            }
        }
    }

    int index()
    {
        return index;
    }

    public boolean intersects(Keys keys)
    {
        return rangeMap.ranges.intersects(keys);
    }

    <R> void process(Function<? super CommandShard, R> function, CompletableFuture<R> future)
    {
        try
        {
            future.complete(function.apply(this));
        }
        catch (Throwable e)
        {
            future.completeExceptionally(e);
        }
    }

    public abstract <R> CompletionStage<R> process(Function<? super CommandShard, R> function);

    public CompletionStage<Void> process(Consumer<? super CommandShard> consumer)
    {
        return process(shard ->
        {
            consumer.accept(shard);
            return null;
        });
    }

    public static class Synchronized extends CommandShard
    {
        public Synchronized(int index)
        {
            super(index);
        }

        @Override
        public synchronized <R> CompletionStage<R> process(Function<? super CommandShard, R> func)
        {
            CompletableFuture<R> future = new CompletableFuture<>();
            process(func, future);
            return future;
        }
    }

    public static class SingleThread extends CommandShard
    {
        private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

        public SingleThread(int index)
        {
            super(index);
            executor.setMaximumPoolSize(1);
        }

        @Override
        public <R> CompletionStage<R> process(Function<? super CommandShard, R> function)
        {
            CompletableFuture<R> future = new CompletableFuture<>();
            executor.execute(() -> process(function, future));
            return future;
        }
    }
}
