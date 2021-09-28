package accord.local;

import accord.api.KeyRange;
import accord.api.Store;
import accord.topology.KeyRanges;
import accord.topology.Shards;
import accord.topology.Topology;
import accord.txn.Keys;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Manages the single threaded metadata shards
 */
public class CommandShards
{
    private Topology localTopology = Shards.EMPTY;
    private final CommandShard[] commandShards;

    public CommandShards(int num, Node node, Store store, CommandShard.Factory shardFactory)
    {
        this.commandShards = new CommandShard[num];
        for (int i=0; i<num; i++)
            commandShards[i] = shardFactory.create(i, node, store);
    }

    public Stream<CommandShard> stream()
    {
        return StreamSupport.stream(new ShardSpliterator(), false);
    }

    public Stream<CommandShard> forKeys(Keys keys)
    {
        // TODO: filter shards before sending to their thread?
        return stream().filter(commandShard -> commandShard.intersects(keys));
    }

    static List<KeyRanges> shardRanges(KeyRanges ranges, int shards)
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
            result.add(new KeyRanges(sharded.get(i).toArray(KeyRange[]::new)));
        }

        return result;
    }

    public synchronized void updateTopology(Topology newTopology)
    {
        KeyRanges removed = localTopology.getRanges().difference(newTopology.getRanges());
        KeyRanges added = newTopology.getRanges().difference(localTopology.getRanges());
        List<KeyRanges> sharded = shardRanges(added, commandShards.length);
        stream().forEach(commands -> commands.updateTopology(newTopology, sharded.get(commands.index()), removed));
    }

    private class ShardSpliterator implements Spliterator<CommandShard>
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

            CompletableFuture<Void>[] futures = new CompletableFuture[commandShards.length - i];
            for (; i<commandShards.length; i++)
                futures[i] = commandShards[i].process(action).toCompletableFuture();

            try
            {
                for (CompletableFuture<Void> future : futures)
                    future.get();
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
            catch (ExecutionException e)
            {
                Throwable cause = e.getCause();
                throw new RuntimeException(cause != null ? cause : e);
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
