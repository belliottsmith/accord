package accord.coordinate.tracking;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Shards;

import java.util.HashSet;
import java.util.Set;

public abstract class AbstractQuorumTracker<T extends AbstractQuorumTracker.QuorumShardTracker> extends AbstractResponseTracker<T>
{
    static class QuorumShardTracker extends AbstractResponseTracker.ShardTracker
    {
        private final Set<Node.Id> outstanding;
        private int success = 0;
        private int failures = 0;
        public QuorumShardTracker(Shard shard)
        {
            super(shard);
            this.outstanding = new HashSet<>(shard.nodes);
        }

        boolean isOutstanding(Node.Id id)
        {
            return outstanding.contains(id);
        }

        public boolean onSuccess(Node.Id id)
        {
            if (!outstanding.remove(id))
                return false;
            success++;
            return true;
        }

        boolean onFailure(Node.Id id)
        {
            if (!outstanding.remove(id))
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
            return !outstanding.isEmpty();
        }
    }

    public AbstractQuorumTracker(Shards shards)
    {
        super(shards);
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
        return allTrackers(QuorumShardTracker::hasReachedQuorum);
    }

    public boolean hasFailed()
    {
        return anyTrackers(QuorumShardTracker::hasFailed);
    }

    public boolean hasOutstandingResponses()
    {
        return anyTrackers(QuorumShardTracker::hasOutstandingResponses);
    }

}
