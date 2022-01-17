package accord.coordinate.tracking;

import accord.coordinate.tracking.AbstractQuorumTracker.QuorumShardTracker;
import accord.local.Node;
import accord.topology.Topologies;

public class QuorumTracker extends AbstractQuorumTracker<QuorumShardTracker>
{
    public QuorumTracker(Topologies topologies)
    {
        super(topologies, QuorumShardTracker[]::new, QuorumShardTracker::new);
    }

    public boolean recordSuccess(Node.Id node)
    {
        return matchingTrackersForNode(node, QuorumShardTracker::onSuccess) > 0;
    }
}
