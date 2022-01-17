package accord.local;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;

import accord.api.*;
import accord.coordinate.Coordinate;
import accord.messages.*;

import javax.annotation.Nullable;

import accord.api.Agent;
import accord.api.Key;
import accord.api.MessageSink;
import accord.api.Result;
import accord.api.ProgressLog;
import accord.api.Scheduler;
import accord.api.DataStore;
import accord.messages.Callback;
import accord.messages.ReplyContext;
import accord.messages.Request;
import accord.messages.Reply;
import accord.topology.KeyRange;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.topology.TopologyManager;
import accord.txn.Ballot;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.apache.cassandra.utils.concurrent.Future;

public class Node implements ConfigurationService.Listener
{
    public static class Id implements Comparable<Id>
    {
        public static final Id NONE = new Id(0);
        public static final Id MAX = new Id(Long.MAX_VALUE);

        public final long id;

        public Id(long id)
        {
            this.id = id;
        }

        @Override
        public int hashCode()
        {
            return Long.hashCode(id);
        }

        @Override
        public boolean equals(Object that)
        {
            return that instanceof Id && equals((Id) that);
        }

        public boolean equals(Id that)
        {
            return id == that.id;
        }

        @Override
        public int compareTo(Id that)
        {
            return Long.compare(this.id, that.id);
        }

        public String toString()
        {
            return Long.toString(id);
        }
    }

    public boolean isCoordinating(TxnId txnId, Ballot promised)
    {
        return promised.node.equals(id) && coordinating.containsKey(txnId);
    }

    public static int numCommandShards()
    {
        return 8; // TODO: make configurable
    }

    private final CommandStores commandStores;
    private final Id id;
    private final MessageSink messageSink;
    private final ConfigurationService configService;
    private final TopologyManager topology;

    private final LongSupplier nowSupplier;
    private final AtomicReference<Timestamp> now;
    private final Agent agent;

    // TODO: this really needs to be thought through some more, as it needs to be per-instance in some cases, and per-node in others
    private final Scheduler scheduler;

    private final Map<TxnId, Future<Result>> coordinating = new ConcurrentHashMap<>();
    private final Set<TxnId> pendingRecovery = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public Node(Id id, MessageSink messageSink, ConfigurationService configService, LongSupplier nowSupplier,
                Supplier<DataStore> dataSupplier, Agent agent, Scheduler scheduler,
                Function<Node, ProgressLog.Factory> progressLogFactory, CommandStores.Factory factory)
    {
        this.id = id;
        this.agent = agent;
        this.messageSink = messageSink;
        this.configService = configService;
        this.topology = new TopologyManager(id, configService::reportEpoch);
        Topology topology = configService.currentTopology();
        this.now = new AtomicReference<>(new Timestamp(topology.epoch(), nowSupplier.getAsLong(), 0, id));
        this.nowSupplier = nowSupplier;
        this.scheduler = scheduler;
        this.commandStores = factory.create(numCommandShards(), this, this::uniqueNow, agent, dataSupplier.get(), scheduler, progressLogFactory.apply(this));

        configService.registerListener(this);
        onTopologyUpdate(topology, false);
    }

    public ConfigurationService configService()
    {
        return configService;
    }

    public MessageSink messageSink()
    {
        return messageSink;
    }

    public long epoch()
    {
        return topology().epoch();
    }

    private synchronized void onTopologyUpdate(Topology topology, boolean acknowledge)
    {
        if (topology.epoch() <= this.topology.epoch())
            return;
        commandStores.updateTopology(topology);
        this.topology.onTopologyUpdate(topology);
        if (acknowledge)
            configService.acknowledgeEpoch(topology.epoch());
    }

    @Override
    public synchronized void onTopologyUpdate(Topology topology)
    {
        onTopologyUpdate(topology, true);
    }

    @Override
    public void onEpochSyncComplete(Id node, long epoch)
    {
        topology.onEpochSyncComplete(node, epoch);
    }

    public TopologyManager topology()
    {
        return topology;
    }

    public void shutdown()
    {
        commandStores.shutdown();
    }

    public Timestamp uniqueNow()
    {
        return now.updateAndGet(cur -> {
            // TODO: this diverges from proof; either show isomorphism or make consistent
            long now = nowSupplier.getAsLong();
            long epoch = Math.max(cur.epoch, topology.epoch());
            return (now > cur.real)
                 ? new Timestamp(epoch, now, 0, id)
                 : new Timestamp(epoch, cur.real, cur.logical + 1, id);
        });
    }

    public Timestamp uniqueNow(Timestamp atLeast)
    {
        if (now.get().compareTo(atLeast) < 0)
            now.accumulateAndGet(atLeast, (current, proposed) -> {
                long minEpoch = topology.epoch();
                current = current.withMinEpoch(minEpoch);
                proposed = proposed.withMinEpoch(minEpoch);
                return proposed.compareTo(current) <= 0 ? current.logicalNext(id) : proposed;
            });
        return uniqueNow();
    }

    public long now()
    {
        return nowSupplier.getAsLong();
    }

    public void forEachLocal(Consumer<CommandStore> forEach)
    {
        commandStores.forEach(forEach);
    }

    public void forEachLocal(Keys keys, Consumer<CommandStore> forEach)
    {
        commandStores.forEach(keys, forEach);
    }

    public void forEachLocal(Txn txn, Consumer<CommandStore> forEach)
    {
        forEachLocal(txn.keys, forEach);
    }

    public void forEachLocal(TxnRequest.Scope scope, Consumer<CommandStore> forEach)
    {
        commandStores.forEach(scope, forEach);
    }

    public <T> T mapReduceLocal(TxnRequest.Scope scope, Function<CommandStore, T> map, BiFunction<T, T, T> reduce)
    {
        return commandStores.mapReduce(scope, map, reduce);
    }

    public <T> T ifLocal(Key key, Function<CommandStore, T> ifLocal)
    {
        return commandStores.mapReduce(key, ifLocal, (a, b) -> { throw new IllegalStateException();} );
    }

    public <T extends Collection<CommandStore>> T collectLocal(Keys keys, IntFunction<T> factory)
    {
        return commandStores.collect(keys, factory);
    }

    public <T extends Collection<CommandStore>> T collectLocal(TxnRequest.Scope scope, IntFunction<T> factory)
    {
        return commandStores.collect(scope, factory);
    }

    // send to every node besides ourselves
    public void send(Topology topology, Request send)
    {
        Set<Id> contacted = new HashSet<>();
        topology.forEach(shard -> send(shard, send, contacted));
    }

    public void send(Shard shard, Request send)
    {
        shard.nodes.forEach(node -> messageSink.send(node, send));
    }

    public void send(Shard shard, Request send, Callback callback)
    {
        shard.nodes.forEach(node -> messageSink.send(node, send, callback));
    }

    private <T> void send(Shard shard, Request send, Set<Id> alreadyContacted)
    {
        shard.nodes.forEach(node -> {
            if (alreadyContacted.add(node))
                send(node, send);
        });
    }

    public <T> void send(Collection<Id> to, Request send)
    {
        for (Id dst: to)
            send(dst, send);
    }

    public <T> void send(Collection<Id> to, Function<Id, Request> requestFactory)
    {
        to.forEach(dst -> send(dst, requestFactory.apply(dst)));
    }

    public <T> void send(Collection<Id> to, Request send, Callback<T> callback)
    {
        for (Id dst: to)
            send(dst, send, callback);
    }

    public <T> void send(Collection<Id> to, Function<Id, Request> requestFactory, Callback<T> callback)
    {
        to.forEach(dst -> send(dst, requestFactory.apply(dst), callback));
    }

    // send to a specific node
    public <T> void send(Id to, Request send, Callback<T> callback)
    {
        messageSink.send(to, send, callback);
    }

    // send to a specific node
    public void send(Id to, Request send)
    {
        messageSink.send(to, send);
    }

    public void reply(Id replyingToNode, ReplyContext replyContext, Reply send)
    {
        messageSink.reply(replyingToNode, replyContext, send);
    }

    public TxnId nextTxnId()
    {
        return new TxnId(uniqueNow());
    }

    public Future<Result> coordinate(TxnId txnId, Txn txn)
    {
        // TODO: The combination of updating the epoch of the next timestamp with epochs we don’t have topologies for,
        //  and requiring preaccept to talk to its topology epoch means that learning of a new epoch via timestamp
        //  (ie not via config service) will halt any new txns from a node until it receives this topology
        if (txnId.epoch > topology().epoch())
        {
            configService.fetchTopologyForEpoch(txnId.epoch);
            return topology().awaitEpoch(txnId.epoch).flatMap(v -> coordinate(txnId, txn));
        }

        Key homeKey = trySelectHomeKey(txn.keys);
        if (homeKey == null)
        {
            homeKey = selectRandomHomeKey();
            txn = new Txn(txn.keys.with(homeKey), txn.read, txn.query, txn.update);
        }
        Future<Result> result = Coordinate.execute(this, txnId, txn, homeKey);
        coordinating.put(txnId, result);
        // TODO (now): if we fail, nominate another node to try instead
        result.addCallback((success, fail) -> coordinating.remove(txnId));
        return result;
    }

    public Future<Result> coordinate(Txn txn)
    {
        return coordinate(nextTxnId(), txn);
    }

    @VisibleForTesting
    public @Nullable Key trySelectHomeKey(Keys keys)
    {
        int i = topology().current().ranges().findFirstIntersecting(keys);
        return i >= 0 ? keys.get(i) : null;
    }

    public Key selectHomeKey(Keys keys)
    {
        Key key = trySelectHomeKey(keys);
        return key != null ? key : selectRandomHomeKey();
    }

    public Key selectRandomHomeKey()
    {
        KeyRanges ranges = topology().current().ranges();
        KeyRange range = ranges.get(ranges.size() == 1 ? 0 : random.nextInt(ranges.size()));
        return range.endInclusive() ? range.end() : range.start();
    }

    // TODO: encapsulate in Coordinate, so we can request that e.g. commits be re-sent?
    public Future<Result> recover(TxnId txnId, Txn txn, Key homeKey)
    {
        if (txnId.epoch > topology.epoch())
        {
            configService.fetchTopologyForEpoch(txnId.epoch);
            topology().awaitEpoch(txnId.epoch).addListener(() -> recover(txnId, txn, homeKey));
            return;
        }

        Future<Result> result = coordinating.get(txnId);
        if (result != null)
            return result;

        result = Coordinate.recover(this, txnId, txn, homeKey);
        coordinating.putIfAbsent(txnId, result);
        result.addCallback((success, fail) -> {
            coordinating.remove(txnId);
            agent.onRecover(this, success, fail);
            // TODO (now): if we fail, nominate another node to try instead
        });
        return result;
    }

    public void receive(Request request, Id from, ReplyContext replyContext)
    {
        long unknownEpoch = topology().maxUnknownEpoch(request);
        if (unknownEpoch > 0)
        {
            configService.fetchTopologyForEpoch(unknownEpoch);
            topology().awaitEpoch(unknownEpoch).addListener(() -> receive(request, from, replyContext));
            return;
        }
        scheduler.now(() -> request.process(this, from, replyContext));
    }

    public boolean isReplicaOf(Key key)
    {
        return commandStores.topology().ranges().contains(key);
    }

    public Scheduler scheduler()
    {
        return scheduler;
    }

    public Agent agent()
    {
        return agent;
    }

    public Id id()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return "Node{" + id + '}';
    }

    @VisibleForTesting
    public CommandStore unsafeForKey(Key key)
    {
        return commandStores.unsafeForKey(key);
    }

}
