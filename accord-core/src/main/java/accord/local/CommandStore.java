package accord.local;

import accord.api.Key;
import accord.api.KeyRange;
import accord.api.Store;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Shards;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.TxnId;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Single threaded internal shard of accord transaction metadata
 */
public abstract class CommandStore
{
    public interface Factory
    {
        CommandStore create(int index, Node node, Store store);
        Factory SYNCHRONIZED = Synchronized::new;
        Factory SINGLE_THREAD = SingleThread::new;
    }

    private final int index;
    private final Node node;
    private final Store store;

    /**
     * maps ranges handled by this command store to their current shards by index
     */
    static class RangeMapping
    {
        private static final RangeMapping EMPTY = new RangeMapping(KeyRanges.EMPTY, new Shard[0], Shards.EMPTY);
        final KeyRanges ranges;
        final Shard[] shards;
        final Topology topology;

        public RangeMapping(KeyRanges ranges, Shard[] shards, Topology topology)
        {
            Preconditions.checkArgument(ranges.size() == shards.length);
            this.ranges = ranges;
            this.shards = shards;
            this.topology = topology;
        }

        private static class Builder
        {
            private final Topology localTopology;
            private final List<KeyRange> ranges;
            private final List<Shard> shards;

            public Builder(int minSize, Topology localTopology)
            {
                this.localTopology = localTopology;
                this.ranges = new ArrayList<>(minSize);
                this.shards = new ArrayList<>(minSize);
            }

            public void addMapping(KeyRange range, Shard shard)
            {
                Preconditions.checkArgument(shard.range.fullyContains(range));
                ranges.add(range);
                shards.add(shard);
            }

            public RangeMapping build()
            {
                return new RangeMapping(new KeyRanges(ranges), shards.toArray(Shard[]::new), localTopology);
            }
        }
    }

    public CommandStore(int index, Node node, Store store)
    {
        this.index = index;
        this.node = node;
        this.store = store;
    }

    private volatile RangeMapping rangeMap = RangeMapping.EMPTY;


    private final NavigableMap<TxnId, Command> commands = new TreeMap<>();
    private final NavigableMap<Key, CommandsForKey> commandsForKey = new TreeMap<>();

    public Command command(TxnId txnId)
    {
        return commands.computeIfAbsent(txnId, id -> new Command(this, id));
    }

    public boolean hasCommand(TxnId txnId)
    {
        return commands.containsKey(txnId);
    }

    public CommandsForKey commandsForKey(Key key)
    {
        return commandsForKey.computeIfAbsent(key, ignore -> new CommandsForKey());
    }

    public boolean hasCommandsForKey(Key key)
    {
        return commandsForKey.containsKey(key);
    }

    public Store store()
    {
        return store;
    }

    public Node node()
    {
        return node;
    }

    public KeyRanges ranges()
    {
        // TODO: check thread safety of callers
        return rangeMap.ranges;
    }

    public Set<Node.Id> nodesFor(Command command)
    {
        RangeMapping mapping = rangeMap;
        Keys keys = command.txn().keys;

        Set<Node.Id> result = new HashSet<>();
        int lowerBound = 0;
        for (int i=0; i<mapping.ranges.size(); i++)
        {
            KeyRange range = mapping.ranges.get(i);
            int lowKeyIdx = range.lowKeyIndex(keys, lowerBound, keys.size());

            // all keys are less than the current range, so no other
            // ranges will intersect with these keys
            if (lowKeyIdx >= keys.size())
                break;

            // all remaining keys are greater than this range, so go to the next one
            if (lowKeyIdx < 0)
                continue;

            // otherwise this range intersects with the txn, so add it's shard's endpoings
            // TODO: filter pending nodes for reads
            result.addAll(mapping.shards[i].nodes);
            lowerBound = lowKeyIdx;
        }

        return result;
    }

    static RangeMapping mapRanges(KeyRanges mergedRanges, Topology localTopology)
    {
        RangeMapping.Builder builder = new RangeMapping.Builder(mergedRanges.size(), localTopology);
        int shardIdx = 0;
        for (int rangeIdx=0; rangeIdx<mergedRanges.size(); rangeIdx++)
        {
            KeyRange mergedRange = mergedRanges.get(rangeIdx);
            while (shardIdx < localTopology.size())
            {
                Shard shard = localTopology.get(shardIdx);

                int cmp = shard.range.compareIntersecting(mergedRange);
                if (cmp < 0)
                    throw new IllegalStateException("mapped shards should always be intersecting or greater than the current shard");

                if (cmp > 0)
                {
                    shardIdx++;
                    continue;
                }

                if (shard.range.fullyContains(mergedRange))
                {
                    builder.addMapping(mergedRange, shard);
                    break;
                }
                else
                {
                    KeyRange intersection = mergedRange.intersection(shard.range);
                    Preconditions.checkState(intersection.start().equals(mergedRange.start()));
                    builder.addMapping(intersection, shard);
                    mergedRange = mergedRange.subRange(intersection.end(), mergedRange.end());
                    shardIdx++;
                }
            }
        }
        return builder.build();
    }

    void updateTopology(Topology topology, KeyRanges added, KeyRanges removed)
    {
        KeyRanges newRanges = rangeMap.ranges.difference(removed).add(added).mergeTouching();
        rangeMap = mapRanges(newRanges, topology);

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

    <R> void process(Function<? super CommandStore, R> function, CompletableFuture<R> future)
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

    void process(Consumer<? super CommandStore> consumer, CompletableFuture<Void> future)
    {
        try
        {
            consumer.accept(this);
            future.complete(null);
        }
        catch (Throwable e)
        {
            future.completeExceptionally(e);
        }
    }

    public abstract <R> CompletionStage<R> process(Function<? super CommandStore, R> function);

    public abstract CompletionStage<Void> process(Consumer<? super CommandStore> consumer);

    public static class Synchronized extends CommandStore
    {
        public Synchronized(int index, Node node, Store store)
        {
            super(index, node, store);
        }

        @Override
        public synchronized <R> CompletionStage<R> process(Function<? super CommandStore, R> func)
        {
            CompletableFuture<R> future = new CompletableFuture<>();
            process(func, future);
            return future;
        }

        @Override
        public synchronized CompletionStage<Void> process(Consumer<? super CommandStore> consumer)
        {
            CompletableFuture<Void> future = new CompletableFuture<>();
            process(consumer, future);
            return future;
        }
    }

    public static class SingleThread extends CommandStore
    {
        private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);

        private class FunctionWrapper<R> extends CompletableFuture<R> implements Runnable
        {
            private final Function<? super CommandStore, R> function;

            public FunctionWrapper(Function<? super CommandStore, R> function)
            {
                this.function = function;
            }

            @Override
            public void run()
            {
                process(function, this);
            }
        }

        private class ConsumerWrapper extends CompletableFuture<Void> implements Runnable
        {
            private final Consumer<? super CommandStore> consumer;

            public ConsumerWrapper(Consumer<? super CommandStore> consumer)
            {
                this.consumer = consumer;
            }

            @Override
            public void run()
            {
                process(consumer, this);
            }
        }

        public SingleThread(int index, Node node, Store store)
        {
            super(index, node, store);
            executor.setMaximumPoolSize(1);
        }

        @Override
        public <R> CompletionStage<R> process(Function<? super CommandStore, R> function)
        {
            FunctionWrapper<R> future = new FunctionWrapper<>(function);
            executor.execute(future);
            return future;
        }

        @Override
        public CompletionStage<Void> process(Consumer<? super CommandStore> consumer)
        {
            ConsumerWrapper future = new ConsumerWrapper(consumer);
            executor.execute(future);
            return future;
        }
    }
}