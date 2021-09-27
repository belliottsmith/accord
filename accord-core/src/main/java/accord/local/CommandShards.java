package accord.local;

import accord.api.KeyRange;
import accord.topology.KeyRanges;
import accord.topology.Shards;
import accord.topology.Topology;
import accord.txn.Keys;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CommandShards
{
    private Topology localTopology = Shards.EMPTY;
    private final CommandShard[] commandShards;

    public CommandShards(int num, Supplier<CommandShard> commandShardConstructor)
    {
        this.commandShards = new CommandShard[num];
        for (int i=0; i<num; i++)
            commandShards[i] = commandShardConstructor.get();
    }

    public Stream<CommandShard> stream()
    {
        return StreamSupport.stream(new CommandSpliterator(), false);
    }

    public Stream<CommandShard> forKeys(Keys keys)
    {
        return stream().filter(commandShard -> commandShard.intersects(keys));
    }

    private static List<KeyRanges> shardRanges(KeyRanges ranges, int shards)
    {
        List<List<KeyRange>> sharded = new ArrayList<>(shards);
        for (int i=0; i<shards; i++)
            sharded.add(new ArrayList<>(ranges.size()));

        for (KeyRange range : ranges)
        {
            KeyRanges split = range.split(shards);
            Preconditions.checkState(split.size() <= shards);
            for (int i=0; i<split.size(); i++)
                sharded.get(i).add(split.get(i));
        }

        List<KeyRanges> result = new ArrayList<>(shards);
        for (int i=0; i<shards; i++)
        {
            result.add(new KeyRanges(sharded.get(i).stream().toArray(KeyRange[]::new)));
        }

        return result;
    }

    public synchronized void updateTopology(Topology newTopology)
    {
        // TODO: maybe support rebalancing between shards
        KeyRanges removed = localTopology.getRanges().difference(newTopology.getRanges());
        if (!removed.isEmpty())
            stream().forEach(commands -> commands.removeRanges(removed));

        KeyRanges added = newTopology.getRanges().difference(localTopology.getRanges());
        if (!added.isEmpty())
        {
            List<KeyRanges> sharded = shardRanges(added, commandShards.length);
            stream().forEach(commands -> commands.addRanges(sharded.get(commands.index())));
        }
    }

    private class CommandSpliterator implements Spliterator<CommandShard>
    {
        int i = 0;

        @Override
        public boolean tryAdvance(Consumer<? super CommandShard> action)
        {
            if (i < commandShards.length)
            {
                CommandShard shard = commandShards[i++];
                try
                {
                    shard.process(action).toCompletableFuture().get();
                }
                catch (InterruptedException | ExecutionException e)
                {
                    throw new RuntimeException(e);
                }

            }
            return i < commandShards.length;
        }

        @Override
        public void forEachRemaining(Consumer<? super CommandShard> action)
        {
            if (i >= commandShards.length)
                return;

            CountDownLatch latch = new CountDownLatch(commandShards.length - i);
            for (; i<commandShards.length; i++)
                commandShards[i].process(action).thenRun(latch::countDown);

            try
            {
                latch.await();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Spliterator<CommandShard> trySplit()
        {
            return null;
        }

        @Override
        public long estimateSize()
        {
            return commandShards.length;
        }

        @Override
        public int characteristics()
        {
            return Spliterator.SIZED | Spliterator.NONNULL | Spliterator.DISTINCT | Spliterator.IMMUTABLE;
        }
    }
}