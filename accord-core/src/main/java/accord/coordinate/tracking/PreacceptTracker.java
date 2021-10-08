package accord.coordinate.tracking;

import accord.local.Node;
import accord.topology.Shard;
import accord.topology.Shards;

public class PreacceptTracker extends FastPathTracker<PreacceptTracker.PreacceptShardTracker>
{
    static class PreacceptShardTracker extends FastPathTracker.FastPathShardTracker
    {
        public PreacceptShardTracker(Shard shard)
        {
            super(shard);
        }

        @Override
        public boolean canIncludeInFastPath(Node.Id node)
        {
            return shard.fastPathElectorate.contains(node);
        }

        @Override
        int fastPathQuorumSize()
        {
            return shard.fastPathQuorumSize;
        }
    }

    public PreacceptTracker(Shards shards)
    {
        super(shards);
    }

    @Override
    PreacceptShardTracker createShardInfo(Shard shard)
    {
        return new PreacceptShardTracker(shard);
    }

    @Override
    PreacceptShardTracker[] createInfoArray(int size)
    {
        return new PreacceptShardTracker[size];
    }
}
