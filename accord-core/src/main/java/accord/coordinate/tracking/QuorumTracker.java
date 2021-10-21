package accord.coordinate.tracking;

import accord.coordinate.tracking.FastPathTracker.FastPathShardTracker;
import accord.topology.Shard;
import accord.topology.Shards;

public class QuorumTracker extends AbstractQuorumTracker<AbstractQuorumTracker.QuorumShardTracker>
{
    public QuorumTracker(Shards shards)
    {
        super(shards, QuorumShardTracker[]::new, QuorumShardTracker::new);
    }
}
