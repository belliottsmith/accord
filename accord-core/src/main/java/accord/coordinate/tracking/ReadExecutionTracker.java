package accord.coordinate.tracking;

import accord.local.Node.Id;
import accord.topology.Shard;
import accord.topology.Shards;
import com.google.common.collect.Sets;

import java.util.*;

public class ReadExecutionTracker extends AbstractResponseTracker<ReadExecutionTracker.ReadExecutionShardTracker>
{
    static class ReadExecutionShardTracker extends AbstractResponseTracker.ShardTracker
    {
        private final Set<Id> candidates;
        private final Set<Id> inflight = new HashSet<>();
        private int failures = 0;
        private boolean hasData = false;

        public ReadExecutionShardTracker(Shard shard)
        {
            super(shard);
            this.candidates = new HashSet<>(shard.nodes);
        }

        public void recordInflightRead(Id node)
        {
            if (candidates.remove(node))
                inflight.add(node);
        }

        public void recordReadSuccess(Id node)
        {
            if (inflight.remove(node))
                hasData = true;
        }

        public boolean shouldRead()
        {
            return !hasData && inflight.isEmpty();
        }

        public void recordReadFailure(Id node)
        {
            inflight.remove(node);
        }

        public boolean hasCompletedRead()
        {
            return hasData;
        }

        public boolean hasFailed()
        {
            return !hasData && candidates.isEmpty() && inflight.isEmpty();
        }
    }

    private final Set<Id> failures = new HashSet<>();

    public ReadExecutionTracker(Shards shards)
    {
        super(shards);
    }

    @Override
    ReadExecutionShardTracker createShardInfo(Shard shard)
    {
        return new ReadExecutionShardTracker(shard);
    }

    @Override
    ReadExecutionShardTracker[] createInfoArray(int size)
    {
        return new ReadExecutionShardTracker[size];
    }

    public void recordInflightRead(Id node)
    {
        applyForNode(node, ReadExecutionShardTracker::recordInflightRead);
    }

    public void recordReadSuccess(Id node)
    {
        applyForNode(node, ReadExecutionShardTracker::recordReadSuccess);
    }

    public void recordReadFailure(Id node)
    {
        applyForNode(node, ReadExecutionShardTracker::recordReadFailure);
        failures.add(node);
    }

    public boolean hasCompletedRead()
    {
        return all(ReadExecutionShardTracker::hasCompletedRead);
    }

    public boolean hasFailed()
    {
        return any(ReadExecutionShardTracker::hasFailed);
    }

    private int intersectionSize(Id node, Set<ReadExecutionShardTracker> target)
    {
        List<ReadExecutionShardTracker> nodeTrackers = trackersForNode(node);
        int count = 0;
        for (int i=0, mi=nodeTrackers.size(); i<mi; i++)
        {
            if (target.contains(nodeTrackers.get(i)))
                count++;
        }
        return count;
    }

    private int compareIntersections(Id left, Id right, Set<ReadExecutionShardTracker> target)
    {
        return Integer.compare(intersectionSize(left, target), intersectionSize(right, target));
    }

    /**
     * Return the smallest set of nodes needed to satisfy required reads.
     *
     * Returns null if the read cannot be completed.
     */
    public Set<Id> computeMinimalReadSet()
    {
        Set<ReadExecutionShardTracker> toRead = accumulate((tracker, accumulate) -> {
            if (!tracker.shouldRead())
                return accumulate;

            if (accumulate == null)
                accumulate = new HashSet<>();

            accumulate.add(tracker);
            return accumulate;
        }, null);

        if (toRead == null || toRead.isEmpty())
            return Collections.emptySet();

        Set<Id> nodes = new HashSet<>();
        Set<Id> candidates = new HashSet<>(Sets.difference(nodes(), failures));
        while (!toRead.isEmpty())
        {
            // TODO: Topology needs concept of locality/distance
            Optional<Id> maxNode = candidates.stream().max((a, b) -> compareIntersections(a, b, toRead));
            if (maxNode.isEmpty())
                return null;

            Id node = maxNode.get();
            nodes.add(node);
            candidates.remove(node);
            List<ReadExecutionShardTracker> nodeTrackers = trackersForNode(node);
            for (int i=0, mi=nodeTrackers.size(); i<mi; i++)
                toRead.remove(nodeTrackers.get(i));
        }

        return nodes;
    }

}
