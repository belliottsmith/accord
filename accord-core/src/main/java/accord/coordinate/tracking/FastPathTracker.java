package accord.coordinate.tracking;

import java.util.function.Function;
import java.util.function.IntFunction;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Shards;

public abstract class FastPathTracker<T extends FastPathTracker.FastPathShardTracker> extends AbstractQuorumTracker<T>
{
    public abstract static class FastPathShardTracker extends AbstractQuorumTracker.QuorumShardTracker
    {
        private int fastPathAccepts = 0;

        public FastPathShardTracker(Shard shard)
        {
            super(shard);
        }

        public abstract boolean canIncludeInFastPath(Node.Id node);

        public void onFastPathSuccess(Node.Id node)
        {
            if (onSuccess(node) && canIncludeInFastPath(node))
                fastPathAccepts++;
        }

        abstract int fastPathQuorumSize();

        boolean hasMetFastPathCriteria()
        {
            return fastPathAccepts >= fastPathQuorumSize();
        }
    }

    public FastPathTracker(Shards shards, IntFunction<T[]> arrayFactory, Function<Shard, T> trackerFactory)
    {
        super(shards, arrayFactory, trackerFactory);
    }

    public void onFastPathSuccess(Node.Id node)
    {
        applyForNode(node, FastPathShardTracker::onFastPathSuccess);
    }

    public void recordSuccess(Node.Id node, boolean fastPath)
    {
        if (fastPath)
            onFastPathSuccess(node);
        else
            recordSuccess(node);
    }

    public boolean hasMetFastPathCriteria()
    {
        return all(FastPathShardTracker::hasMetFastPathCriteria);
    }
}
