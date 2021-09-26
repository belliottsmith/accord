package accord.local;

import accord.topology.Shards;
import accord.topology.Topology;
import accord.txn.Keys;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class CommandShards
{
    private Topology topology = Shards.EMPTY;
    private final CommandShard[] commandShards;
    private final ReadWriteLock topologyLock = new ReentrantReadWriteLock(true);

    public CommandShards(int num)
    {
        this.commandShards = new CommandShard[num];
        for (int i=0; i<num; i++)
            commandShards[i] = new CommandShard();
    }

    public Stream<CommandShard> stream()
    {
        LockingSpliterator spliterator = new LockingSpliterator();
        return StreamSupport.stream(spliterator, false).onClose(spliterator::close);
    }

    public Stream<CommandShard> forKeys(Keys keys)
    {
        return stream().filter(commandShard -> commandShard.intersects(keys));
    }

    public void onTopologyChange(Topology newTopology)
    {
        // TODO: change topology without stopping all activity
        topologyLock.writeLock().lock();
        try
        {
            throw new UnsupportedOperationException();
        }
        finally
        {
            topologyLock.writeLock().unlock();
        }
    }

    private class LockingSpliterator extends Spliterators.AbstractSpliterator<CommandShard>
    {
        Iterator<CommandShard> iterator = null;
        boolean isClosed = false;

        public LockingSpliterator()
        {
            super(commandShards.length, Spliterator.SIZED | Spliterator.NONNULL | Spliterator.DISTINCT | Spliterator.IMMUTABLE);
        }

        @Override
        public boolean tryAdvance(Consumer<? super CommandShard> action)
        {
            Preconditions.checkState(!isClosed);
            if (iterator == null)
            {
                topologyLock.readLock().lock();
                iterator = Iterators.forArray(commandShards);
            }

            if (!iterator.hasNext())
                return false;

            action.accept(iterator.next());
            return iterator.hasNext();
        }

        void close()
        {
            if (iterator != null)
                topologyLock.readLock().unlock();
            isClosed = false;
        }
    }
}
