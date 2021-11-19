package accord.burn;

import accord.api.KeyRange;
import accord.api.TestableConfigurationService;
import accord.local.Command;
import accord.local.Node;
import accord.local.Status;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.utils.MessageTask;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class TopologyUpdate
{
    private static final Logger logger = LoggerFactory.getLogger(TopologyUpdate.class);

    private static class CountDownFuture<T> extends CompletableFuture<T>
    {
        private final AtomicInteger remaining;
        private final T value;

        public CountDownFuture(int count, T value)
        {
            this.remaining = new AtomicInteger(count);
            this.value = value;
        }

        public CountDownFuture(int count)
        {
            this(count, null);
        }

        public void countDown()
        {
            if (remaining.decrementAndGet() == 0)
                complete(value);
        }
    }

    private static class CommandSync
    {
        private final TxnId txnId;
        private final Txn txn;
        private final Timestamp executeAt;

        private final Dependencies deps;

        public CommandSync(Command command)
        {
            Preconditions.checkArgument(command.hasBeen(Status.Committed));
            this.txnId = command.txnId();
            this.txn = command.txn();
            this.executeAt = command.executeAt();
            this.deps = command.savedDeps();
        }
        public void process(Node node)
        {
            node.local(txn.keys()).forEach(commandStore -> commandStore.command(txnId).commit(txn, deps, executeAt));
        }
    }

    public static <T> Function<Throwable, T> dieOnException()
    {
        return throwable -> {
            logger.error("", throwable);
            System.exit(1);
            return null;
        };
    }

    public static <T> CompletionStage<T> dieExceptionally(CompletionStage<T> stage)
    {
        return stage.exceptionally(dieOnException());
    }

    private static <T> CompletionStage<Void> map(Collection<T> items, Function<T, CompletionStage<Void>> function)
    {
        CountDownFuture<Void> latch = new CountDownFuture<>(items.size());
        for (T item : items)
        {
            function.apply(item).thenRun(latch::countDown);
        }
        return latch;
    }

    public static MessageTask notify(Node originator, Collection<Node.Id> cluster, Topology update)
    {
        return MessageTask.begin(originator, cluster, "TopologyNotify:" + update.epoch(), (node, from) -> {
            long nodeEpoch = node.topology().epoch();
            if (nodeEpoch + 1 < update.epoch())
                return false;
            ((TestableConfigurationService) node.configService()).reportTopology(update);
            return true;
        });
    }

    private static CompletionStage<Void> broadcast(List<Node.Id> cluster, Function<Node.Id, Node> lookup, String desc, BiConsumer<Node, Node.Id> process)
    {
        return map(cluster, node -> MessageTask.apply(lookup.apply(node), cluster, desc, process));
    }

    private static Collection<Node.Id> allNodesFor(Txn txn, Topology... topologies)
    {
        Set<Node.Id> result = new HashSet<>();
        for (Topology topology : topologies)
            result.addAll(topology.forKeys(txn.keys()).nodes());
        return result;
    }

    private static Stream<MessageTask> syncEpochCommands(Node node, long epoch, KeyRanges ranges, Function<CommandSync, Collection<Node.Id>> recipients)
    {
        Map<TxnId, CommandSync> syncMessages = new ConcurrentHashMap<>();
        Consumer<Command> commandConsumer = command -> syncMessages.put(command.txnId(), new CommandSync(command));
        node.local().forEach(commandStore -> commandStore.forCommittedInEpoch(ranges, epoch, commandConsumer));
        return syncMessages.values().stream().map(cmd -> MessageTask.of(node, recipients.apply(cmd), "Sync:" + cmd.txnId + ':' + epoch, cmd::process));
    }

    /**
     * Syncs all replicated commands. Overkill, but useful for confirming issues in optimizedSync
     */
    private static Stream<MessageTask> thoroughSync(Node node, long syncEpoch)
    {
        long nextEpoch = syncEpoch + 1;
        Topology syncTopology = node.configService().getTopologyForEpoch(syncEpoch);
        Topology localTopology = syncTopology.forNode(node.id());
        Topology nextTopology = node.configService().getTopologyForEpoch(nextEpoch);
        Function<CommandSync, Collection<Node.Id>> allNodes = cmd -> allNodesFor(cmd.txn, syncTopology, nextTopology);

        KeyRanges ranges = localTopology.ranges();
        Stream<MessageTask> messageStream = Stream.empty();
        for (long epoch=1; epoch<=syncEpoch; epoch++)
        {
            messageStream = Stream.concat(messageStream, syncEpochCommands(node, epoch, ranges, allNodes));
        }
        return messageStream;
    }

    /**
     * Syncs all newly replicated commands when nodes are gaining ranges and the current epoch
     * FIXME: causes correctness issues after multiple ownership changes, unclear why
     */
    private static Stream<MessageTask> optimizedSync(Node node, long syncEpoch)
    {
        long nextEpoch = syncEpoch + 1;
        Topology syncTopology = node.configService().getTopologyForEpoch(syncEpoch);
        Topology localTopology = syncTopology.forNode(node.id());
        Topology nextTopology = node.configService().getTopologyForEpoch(nextEpoch);
        Function<CommandSync, Collection<Node.Id>> allNodes = cmd -> allNodesFor(cmd.txn, syncTopology, nextTopology);
        Function<CommandSync, Collection<Node.Id>> nextNodes = cmd -> allNodesFor(cmd.txn, nextTopology);

        // backfill new replicas with operations from prior epochs
        Stream<MessageTask> messageStream = Stream.empty();
        for (Shard syncShard : localTopology)
        {
            for (Shard nextShard : nextTopology)
            {
                // do nothing if there's no change
                if (syncShard.range.equals(nextShard.range) && syncShard.nodeSet.equals(nextShard.nodeSet))
                    continue;

                KeyRange intersection = syncShard.range.intersection(nextShard.range);

                if (intersection == null)
                    continue;

                KeyRanges ranges = KeyRanges.singleton(intersection);
                for (long epoch=1; epoch<syncEpoch; epoch++)
                    messageStream = Stream.concat(messageStream, syncEpochCommands(node,
                                                                                   epoch,
                                                                                   ranges,
//                                                                                   nextNodes));
                                                                                   allNodes));
            }
        }

        // update all current and future replicas with the contents of the sync epoch
        messageStream = Stream.concat(messageStream, syncEpochCommands(node,
                                                                       syncEpoch,
                                                                       localTopology.ranges(),
                                                                       allNodes));
        return messageStream;
    }

    public static CompletionStage<Void> sync(Node node, long syncEpoch)
    {
        Stream<MessageTask> messageStream = thoroughSync(node, syncEpoch);
//        Stream<MessageTask> messageStream = optimizedSync(node, syncEpoch);

        Iterator<MessageTask> iter = messageStream.iterator();
        if (!iter.hasNext())
        {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.complete(null);
            return future;
        }

        MessageTask first = iter.next();
        MessageTask last = first;
        while (iter.hasNext())
        {
            MessageTask next = iter.next();
            last.thenRun(next);
            last = next;
        }

        first.run();
        return dieExceptionally(last);
    }

    public static void update(Node originator, Topology update, List<Node.Id> cluster, Function<Node.Id, Node> lookup)
    {
        long epoch = update.epoch();
        // notify
        dieExceptionally(notify(originator, cluster, update)
                // acknowledge
                .thenCompose(v -> broadcast(cluster, lookup, "EpochAcknowledge:" + epoch, (node, from) -> node.onEpochAcknowledgement(from, epoch)))
                // sync operations
                .thenCompose(v -> map(cluster, node -> sync(lookup.apply(node), epoch - 1)))
                // inform sync complete
                .thenCompose(v -> broadcast(cluster, lookup, "SyncComplete:" + epoch, (node, from) -> node.onEpochSyncComplete(from, epoch))));
    }
    public static CompletionStage<Void> acknowledgeAndSync(Node originator, long epoch, Collection<Node.Id> cluster)
    {
        return dieExceptionally(MessageTask.apply(originator, cluster, "EpochAcknowledge:" + epoch, (node, from) -> node.onEpochAcknowledgement(from, epoch))
                .thenCompose(v -> sync(originator, epoch - 1))
                .thenCompose(v -> MessageTask.apply(originator, cluster, "SyncComplete:" + epoch, (node, from) -> node.onEpochSyncComplete(from, epoch))));
    }
}