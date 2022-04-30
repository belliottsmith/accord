package accord.local;

import accord.api.Agent;
import accord.api.Key;
import accord.api.DataStore;
import accord.messages.TxnRequest;
import accord.api.ProgressLog;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Timestamp;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.*;

import static java.lang.Boolean.FALSE;

/**
 * Manages the single threaded metadata shards
 */
public abstract class CommandStores
{
    public interface Factory
    {
        CommandStores create(int num,
                             Node node,
                             Function<Timestamp, Timestamp> uniqueNow,
                             Agent agent,
                             DataStore store,
                             ProgressLog.Factory progressLogFactory);
    }

    interface Fold<I1, I2, O>
    {
        O fold(CommandStore store, I1 i1, I2 i2, O accumulator);
    }

    private static class Supplier
    {
        private final Node node;
        private final Function<Timestamp, Timestamp> uniqueNow;
        private final Agent agent;
        private final DataStore store;
        private final ProgressLog.Factory progressLogFactory;
        private final CommandStore.Factory shardFactory;
        private final int numShards;

        Supplier(Node node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory, int numShards)
        {
            this.node = node;
            this.uniqueNow = uniqueNow;
            this.agent = agent;
            this.store = store;
            this.progressLogFactory = progressLogFactory;
            this.shardFactory = shardFactory;
            this.numShards = numShards;
        }

        private CommandStore create(int generation, int shardIndex, KeyRanges ranges)
        {
            return shardFactory.create(generation, shardIndex, numShards, node, uniqueNow, agent, store, progressLogFactory, ranges);
        }

        private CommandStores updateTopology(CommandStores stores, Topology newTopology)
        {
            Preconditions.checkArgument(!newTopology.isSubset(), "Use full topology for CommandStores.updateTopology");

            CommandStores current = stores;
            if (newTopology.epoch() <= current.global.epoch())
                return stores;

            Topology newLocalTopology = newTopology.forNode(node.id());
            KeyRanges added = newLocalTopology.ranges().difference(current.local.ranges());

            for (StoreGroup group : stores.groups)
            {
                // FIXME: remove this (and the corresponding check in TopologyRandomizer) once lower bounds are implemented.
                //  In the meantime, the logic needed to support acquiring ranges that we previously replicated is pretty
                //  convoluted without the ability to jettison epochs.
                Preconditions.checkState(!group.ranges.intersects(added));
            }

            if (added.isEmpty())
                return stores.update(stores.groups, newTopology, newLocalTopology);

            int newGeneration = current.groups.length;
            StoreGroup[] newGroups = new StoreGroup[current.groups.length + 1];
            CommandStore[] newStores = new CommandStore[numShards];
            System.arraycopy(current.groups, 0, newGroups, 0, current.groups.length);

            for (int i=0; i<numShards; i++)
                newStores[i] = create(newGeneration, i, added);

            newGroups[current.groups.length] = new StoreGroup(newStores, added);

            return stores.update(newGroups, newTopology, newLocalTopology);
        }

    }

    static class StoreGroup
    {
        final CommandStore[] stores;
        final KeyRanges ranges;

        public StoreGroup(CommandStore[] stores, KeyRanges ranges)
        {
            Preconditions.checkArgument(stores.length <= 64);
            this.stores = stores;
            this.ranges = ranges;
        }

        long all()
        {
            return -1L >>> (64 - stores.length);
        }

        long matches(Keys keys)
        {
            return keys.foldl(ranges, StoreGroup::addKeyIndex, stores.length, 0L, -1L);
        }

        long matches(Key key)
        {
            int index = ranges.rangeIndexForKey(key);
            return index < 0 ? 0L : 1L << index;
        }

        long matches(TxnRequest.Scope scope)
        {
            return matches(scope.keys());
        }

        static long keyIndex(Key key, long numShards)
        {
            return Integer.toUnsignedLong(key.keyHash()) % numShards;
        }

        private static long addKeyIndex(Key key, long numShards, long accumulate)
        {
            return accumulate | (1L << keyIndex(key, numShards));
        }
    }

    final Supplier supplier;

    final StoreGroup[] groups;
    final Topology global;
    final Topology local;
    final int size;

    private CommandStores(Supplier supplier, StoreGroup[] groups, Topology global, Topology local)
    {
        this.supplier = supplier;
        this.groups = groups;
        this.global = global;
        this.local = local;
        int size = 0;
        for (StoreGroup group : groups)
            size += group.stores.length;
        this.size = size;
    }

    abstract CommandStores update(StoreGroup[] groups, Topology global, Topology local);

    public Topology local()
    {
        return local;
    }

    public Topology global()
    {
        return global;
    }

    public int size()
    {
        return size;
    }

    public CommandStores(int num, Node node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, DataStore store,
                         ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
    {
        this(new Supplier(node, uniqueNow, agent, store, progressLogFactory, shardFactory, num), new StoreGroup[0], Topology.EMPTY, Topology.EMPTY);
    }

    public synchronized void shutdown()
    {
        for (StoreGroup group : groups)
            for (CommandStore commandStore : group.stores)
                commandStore.shutdown();
    }

    protected abstract <S> void forEach(ToLongBiFunction<StoreGroup, S> select, S scope, Consumer<? super CommandStore> forEach);
    protected abstract <S, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce);

    public void forEach(Consumer<CommandStore> forEach)
    {
        forEach((s, i) -> s.all(), null, forEach);
    }

    public void forEach(Keys keys, Consumer<CommandStore> forEach)
    {
        forEach(StoreGroup::matches, keys, forEach);
    }

    public void forEach(TxnRequest.Scope scope, Consumer<CommandStore> forEach)
    {
        forEach(StoreGroup::matches, scope, forEach);
    }

    public <T> T mapReduce(TxnRequest.Scope scope, Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return mapReduce(StoreGroup::matches, scope, map, reduce);
    }

    public <T> T mapReduce(Key key, Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return mapReduce(StoreGroup::matches, key, map, reduce);
    }

    public <T extends Collection<CommandStore>> T collect(Keys keys, IntFunction<T> factory)
    {
        return foldl(StoreGroup::matches, keys, CommandStores::append, null, null, factory);
    }

    public <T extends Collection<CommandStore>> T collect(TxnRequest.Scope scope, IntFunction<T> factory)
    {
        return foldl(StoreGroup::matches, scope, CommandStores::append, null, null, factory);
    }

    public CommandStores withNewTopology(Topology newTopology)
    {
        return supplier.updateTopology(this, newTopology);
    }

    private static <T extends Collection<CommandStore>> T append(CommandStore store, Object ignore1, Object ignore2, T to)
    {
        to.add(store);
        return to;
    }

    private <I1, I2, O> O foldl(int startGroup, long bitset, Fold<? super I1, ? super I2, O> fold, I1 param1, I2 param2, O accumulator)
    {
        int groupIndex = startGroup;
        StoreGroup group = groups[groupIndex];
        int offset = 0;
        while (true)
        {
            int i = Long.numberOfTrailingZeros(bitset) - offset;
            while (i < group.stores.length)
            {
                accumulator = fold.fold(group.stores[i], param1, param2, accumulator);
                bitset ^= Long.lowestOneBit(bitset);
                i = Long.numberOfTrailingZeros(bitset) - offset;
            }

            if (++groupIndex == groups.length)
                break;

            if (bitset == 0)
                break;

            offset += group.stores.length;
            group = groups[groupIndex];
            if (offset + group.stores.length > 64)
                break;
        }
        return accumulator;
    }

    <S, I1, I2, O> O foldl(ToLongBiFunction<StoreGroup, S> select, S scope, Fold<? super I1, ? super I2, O> fold, I1 param1, I2 param2, IntFunction<? extends O> factory)
    {
        O accumulator = null;
        int startGroup = 0;
        while (startGroup < groups.length)
        {
            long bits = select.applyAsLong(groups[startGroup], scope);
            if (bits == 0)
            {
                ++startGroup;
                continue;
            }

            int offset = groups[startGroup].stores.length;
            int endGroup = startGroup + 1;
            while (endGroup < groups.length)
            {
                StoreGroup group = groups[endGroup];
                if (offset + group.stores.length > 64)
                    break;

                bits += select.applyAsLong(group, scope) << offset;
                offset += group.stores.length;
                ++endGroup;
            }

            if (accumulator == null)
                accumulator = factory.apply(Long.bitCount(bits));

            accumulator = foldl(startGroup, bits, fold, param1, param2, accumulator);
            startGroup = endGroup;
        }

        return accumulator;
    }

    @VisibleForTesting
    public CommandStore unsafeForKey(Key key)
    {
        for (StoreGroup group : groups)
        {
            if (group.ranges.contains(key))
            {
                for (CommandStore store : group.stores)
                {
                    if (store.hashIntersects(key))
                        return store;
                }
            }
        }
        throw new IllegalArgumentException();
    }

    public static class Synchronized extends CommandStores
    {
        public Synchronized(int num, Node node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, uniqueNow, agent, store, progressLogFactory, CommandStore.Synchronized::new);
        }

        public Synchronized(Supplier supplier, StoreGroup[] groups, Topology global, Topology local)
        {
            super(supplier, groups, global, local);
        }

        @Override
        protected <S, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce)
        {
            return foldl(select, scope, (store, f, r, t) -> t == null ? f.apply(store) : r.apply(t, f.apply(store)), map, reduce, ignore -> null);
        }

        @Override
        CommandStores update(StoreGroup[] groups, Topology global, Topology local)
        {
            return new Synchronized(supplier, groups, global, local);
        }

        @Override
        protected <S> void forEach(ToLongBiFunction<StoreGroup, S> select, S scope, Consumer<? super CommandStore> forEach)
        {
            foldl(select, scope, (store, f, r, t) -> { f.accept(store); return null; }, forEach, null, ignore -> FALSE);
        }
    }

    public static class SingleThread extends CommandStores
    {
        public SingleThread(int num, Node node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            this(num, node, uniqueNow, agent, store, progressLogFactory, CommandStore.SingleThread::new);
        }

        public SingleThread(Supplier supplier, StoreGroup[] groups, Topology global, Topology local)
        {
            super(supplier, groups, global, local);
        }

        @Override
        CommandStores update(StoreGroup[] groups, Topology global, Topology local)
        {
            return new SingleThread(supplier, groups, global, local);
        }

        public SingleThread(int num, Node node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.Factory shardFactory)
        {
            super(num, node, uniqueNow, agent, store, progressLogFactory, shardFactory);
        }

        private <S, F, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, F f, Fold<F, ?, List<Future<T>>> fold, BiFunction<T, T, T> reduce)
        {
            List<Future<T>> futures = foldl(select, scope, fold, f, null, ArrayList::new);
            T result = null;
            for (Future<T> future : futures)
            {
                try
                {
                    T next = future.get();
                    if (result == null) result = next;
                    else result = reduce.apply(result, next);
                }
                catch (InterruptedException e)
                {
                    throw new UncheckedInterruptedException(e);
                }
                catch (ExecutionException e)
                {
                    throw new RuntimeException(e.getCause());
                }
            }
            return result;
        }

        @Override
        protected <S, T> T mapReduce(ToLongBiFunction<StoreGroup, S> select, S scope, Function<? super CommandStore, T> map, BiFunction<T, T, T> reduce)
        {
            return mapReduce(select, scope, map, (store, f, i, t) -> { t.add(store.process(f)); return t; }, reduce);
        }

        protected <S> void forEach(ToLongBiFunction<StoreGroup, S> select, S scope, Consumer<? super CommandStore> forEach)
        {
            mapReduce(select, scope, forEach, (store, f, i, t) -> { t.add(store.process(f)); return t; }, (Void i1, Void i2) -> null);
        }
    }

    public static class Debug extends SingleThread
    {
        public Debug(int num, Node node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory)
        {
            super(num, node, uniqueNow, agent, store, progressLogFactory, CommandStore.Debug::new);
        }

        public Debug(Supplier supplier, StoreGroup[] groups, Topology global, Topology local)
        {
            super(supplier, groups, global, local);
        }

        @Override
        CommandStores update(StoreGroup[] groups, Topology global, Topology local)
        {
            return new Debug(supplier, groups, global, local);
        }
    }

}
