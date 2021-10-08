package accord.coordinate.tracking;

import accord.Utils;
import accord.impl.TopologyUtils;
import accord.local.Node;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Shards;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.shards;

public class PreacceptTrackerTest
{
    private static final Node.Id[] ids = Utils.ids(5).toArray(Node.Id[]::new);
    private static final KeyRanges ranges = TopologyUtils.initialRanges(5, 500);
    private static final Shards topology = TopologyUtils.initialTopology(ids, ranges, 3);
        /*
        (000, 100](100, 200](200, 300](300, 400](400, 500]
        [1, 2, 3] [2, 3, 4] [3, 4, 5] [4, 5, 1] [5, 1, 2]
         */

    private static void assertResponseState(PreacceptTracker responses,
                                            boolean quorumReached,
                                            boolean fastPathAccepted,
                                            boolean failed,
                                            boolean hasOutstandingResponses)
    {
        Assertions.assertEquals(quorumReached, responses.hasReachedQuorum());
        Assertions.assertEquals(fastPathAccepted, responses.hasMetFastPathCriteria());
        Assertions.assertEquals(failed, responses.hasFailed());
        Assertions.assertEquals(hasOutstandingResponses, responses.hasOutstandingResponses());
    }

    @Test
    void singleShard()
    {
        Shards subShards = shards(topology.get(0));
        PreacceptTracker responses = new PreacceptTracker(subShards);

        responses.recordSuccess(ids[0]);
        assertResponseState(responses, false, false, false, true);

        responses.recordSuccess(ids[1]);
        assertResponseState(responses, true, false, false, true);

        responses.recordSuccess(ids[2]);
        assertResponseState(responses, true, false, false, false);
    }

    @Test
    void singleShardFastPath()
    {
        Shards subShards = shards(topology.get(0));
        PreacceptTracker responses = new PreacceptTracker(subShards);

        responses.onFastPathSuccess(ids[0]);
        assertResponseState(responses, false, false, false, true);

        responses.onFastPathSuccess(ids[1]);
        assertResponseState(responses, true, false, false, true);

        responses.onFastPathSuccess(ids[2]);
        assertResponseState(responses, true, true, false, false);
    }

    /**
     * responses from unexpected endpoints should be ignored
     */
    @Test
    void unexpectedResponsesAreIgnored()
    {
        Shards subShards = shards(topology.get(0));
        PreacceptTracker responses = new PreacceptTracker(subShards);

        responses.recordSuccess(ids[0]);
        assertResponseState(responses, false, false, false, true);

        responses.recordSuccess(ids[1]);
        assertResponseState(responses, true, false, false, true);

        Assertions.assertFalse(subShards.get(0).nodes.contains(ids[4]));
        responses.recordSuccess(ids[4]);
        assertResponseState(responses, true, false, false, true);
    }

    @Test
    void failure()
    {
        Shards subShards = shards(topology.get(0));
        PreacceptTracker responses = new PreacceptTracker(subShards);

        responses.onFastPathSuccess(ids[0]);
        assertResponseState(responses, false, false, false, true);

        responses.recordFailure(ids[1]);
        assertResponseState(responses, false, false, false, true);

        responses.recordFailure(ids[2]);
        assertResponseState(responses, false, false, true, false);
    }

    @Test
    void multiShard()
    {
        Shards subShards = new Shards(new Shard[]{topology.get(0), topology.get(1), topology.get(2)});
        PreacceptTracker responses = new PreacceptTracker(subShards);
        /*
        (000, 100](100, 200](200, 300]
        [1, 2, 3] [2, 3, 4] [3, 4, 5]
         */

        Assertions.assertSame(subShards.get(0), responses.getUnsafe(0).shard);
        Assertions.assertSame(subShards.get(1), responses.getUnsafe(1).shard);
        Assertions.assertSame(subShards.get(2), responses.getUnsafe(2).shard);

        responses.onFastPathSuccess(ids[1]);
        assertResponseState(responses, false, false, false, true);

        responses.onFastPathSuccess(ids[2]);
        assertResponseState(responses, false, false, false, true);

        responses.onFastPathSuccess(ids[3]);
        // the middle shard will have reached fast path
        Assertions.assertTrue(responses.getUnsafe(1).hasMetFastPathCriteria());
        // but since the others haven't, it won't report it as accepted
        assertResponseState(responses, true, false, false, true);

        responses.onFastPathSuccess(ids[0]);
        responses.onFastPathSuccess(ids[4]);
        assertResponseState(responses, true, true, false, false);
    }
}
