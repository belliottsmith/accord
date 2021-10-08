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
        super(shards);
    }

    @Override
    RecoveryShardTracker createShardInfo(Shard shard)
    {
        return new RecoveryShardTracker(shard);
    }

    @Override
    RecoveryShardTracker[] createInfoArray(int size)
    {
        return new RecoveryShardTracker[size];
    }
}
