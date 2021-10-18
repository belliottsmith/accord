package accord.coordinate.tracking;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Shards;
import com.google.common.annotations.VisibleForTesting;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Predicate;

abstract class AbstractResponseTracker<T extends AbstractResponseTracker.ShardTracker>
{
    private final Shards shards;
    private final Map<Node.Id, List<T>> nodeMap = new HashMap<>();
    private final T[] trackers;

    static class ShardTracker
    {
        final Shard shard;

        public ShardTracker(Shard shard)
        {
            this.shard = shard;
        }
    }

    private static <T extends ShardTracker> void indexNodes(T tracker, Map<Node.Id, List<T>> nodeMap, int maxTrackers)
    {
        List<Node.Id> nodes = tracker.shard.nodes;
        for (int i=0, mi=nodes.size(); i<mi; i++)
        {
            Node.Id node = nodes.get(i);
            List<T> trackers = nodeMap.get(node);
            if (trackers == null)
            {
                trackers = new ArrayList<>(maxTrackers);
                nodeMap.put(node, trackers);
            }
            trackers.add(tracker);
        }
    }

    public AbstractResponseTracker(Shards shards)
    {
        this.shards = shards;
        trackers = createInfoArray(shards.size());
        this.shards.forEach((i, shard) -> {
            trackers[i] = createShardInfo(shard);
            indexNodes(trackers[i], nodeMap, shards.size());
        });
    }

    abstract T createShardInfo(Shard shard);
    abstract T[] createInfoArray(int size);

    void applyForNode(Node.Id node, BiConsumer<T, Node.Id> consumer)
    {
        for (T tracker : trackers)
            consumer.accept(tracker, node);
    }

    boolean all(Predicate<T> predicate)
    {
        for (T tracker : trackers)
            if (!predicate.test(tracker))
                return false;
        return true;
    }

    boolean any(Predicate<T> predicate)
    {
        for (T tracker : trackers)
            if (predicate.test(tracker))
                return true;
        return false;
    }

    <V> V accumulate(BiFunction<T, V, V> function, V start)
    {
        for (T tracker : trackers)
            start = function.apply(tracker, start);
        return start;
    }

    List<T> trackersForNode(Node.Id node)
    {
        return nodeMap.get(node);
    }

    public Set<Node.Id> nodes()
    {
        return nodeMap.keySet();
    }

    @VisibleForTesting
    T unsafeGet(int i)
    {
        return trackers[i];
    }
}
