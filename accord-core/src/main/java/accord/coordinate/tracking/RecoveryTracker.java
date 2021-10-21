package accord.coordinate.tracking;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Shards;

public class RecoveryTracker extends FastPathTracker<RecoveryTracker.RecoveryShardTracker>
{
    static class RecoveryShardTracker extends FastPathTracker.FastPathShardTracker
    {
        public RecoveryShardTracker(Shard shard)
        {
            super(shard);
        }

        @Override
        public boolean canIncludeInFastPath(Node.Id node)
        {
            return true;
        }

        @Override
        int fastPathQuorumSize()
        {
            return shard.recoveryFastPathSize;
        }
    }

    public RecoveryTracker(Shards shards)
    {
        super(shards, RecoveryShardTracker[]::new, RecoveryShardTracker::new);
    }
}
