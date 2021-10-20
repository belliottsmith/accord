package accord.coordinate.tracking;

import accord.topology.Shard;
import accord.topology.Shards;

public class QuorumTracker extends AbstractQuorumTracker<AbstractQuorumTracker.QuorumShardTracker>
{
    public QuorumTracker(Shards shards)
    {
        super(shards);
    }

    @Override
    QuorumShardTracker createShardTracker(Shard shard)
    {
        return new QuorumShardTracker(shard);
    }

    @Override
    QuorumShardTracker[] createShardTrackerArray(int size)
    {
        return new QuorumShardTracker[size];
    }
}
