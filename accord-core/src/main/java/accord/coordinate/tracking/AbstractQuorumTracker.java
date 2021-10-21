package accord.coordinate.tracking;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Shards;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

public abstract class AbstractQuorumTracker<T extends AbstractQuorumTracker.QuorumShardTracker> extends AbstractResponseTracker<T>
{
    static class QuorumShardTracker extends AbstractResponseTracker.ShardTracker
    {
        private final Set<Node.Id> inflight;
        private int success = 0;
        private int failures = 0;
        public QuorumShardTracker(Shard shard)
        {
            super(shard);
            this.inflight = new HashSet<>(shard.nodes);
        }

        boolean hasInflight(Node.Id id)
        {
            return inflight.contains(id);
        }

        public boolean onSuccess(Node.Id id)
        {
            if (!inflight.remove(id))
                return false;
            success++;
            return true;
        }

        boolean onFailure(Node.Id id)
        {
            if (!inflight.remove(id))
                return false;
            failures++;
            return true;
        }

        boolean hasFailed()
        {
            return failures >= shard.slowPathQuorumSize;
        }

        boolean hasReachedQuorum()
        {
            return success >= shard.slowPathQuorumSize;
        }

        boolean hasOutstandingResponses()
        {
            return !inflight.isEmpty();
        }
    }

    public AbstractQuorumTracker(Shards shards, IntFunction<T[]> arrayFactory, Function<Shard, T> trackerFactory)
    {
        super(shards, arrayFactory, trackerFactory);
    }

    public void recordSuccess(Node.Id node)
    {
        applyForNode(node, QuorumShardTracker::onSuccess);
    }

    // TODO: refactor to return true if this call caused the state change to failed
    public void recordFailure(Node.Id node)
    {
        applyForNode(node, QuorumShardTracker::onFailure);
    }

    public boolean hasReachedQuorum()
    {
        return all(QuorumShardTracker::hasReachedQuorum);
    }

    public boolean hasFailed()
    {
        return any(QuorumShardTracker::hasFailed);
    }

    public boolean hasOutstandingResponses()
    {
        return any(QuorumShardTracker::hasOutstandingResponses);
    }

}
