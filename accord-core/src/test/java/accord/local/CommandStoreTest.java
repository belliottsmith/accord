package accord.local;

import accord.Utils;
import accord.api.KeyRange;
import accord.impl.IntKey;
import accord.impl.TopologyUtils;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Topology;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class CommandStoreTest
{
    @Test
    void topologyChangeTest()
    {
        // TODO: test range addition
        // TODO: test range removal and metadata cleanup

    }

    private static Shard[] shards(Topology topology, int... indexes)
    {
        Shard[] shards = new Shard[indexes.length];

        for (int i=0; i<indexes.length; i++)
            shards[i] = topology.get(indexes[i]);
        return shards;
    }

    private static void assertMapping(KeyRanges ranges, Shard[] shards, CommandStore.RangeMapping mapping)
    {
        Assertions.assertEquals(ranges, mapping.ranges);
        Assertions.assertArrayEquals(shards, mapping.shards);
    }

    private static KeyRanges ranges(KeyRange... ranges)
    {
        return new KeyRanges(ranges);
    }

    private static KeyRange<IntKey> r(int start, int end)
    {
        return IntKey.range(start, end);
    }

    @Test
    void mapRangesTest()
    {
        List<Node.Id> ids = Utils.ids(5);
        KeyRanges ranges = TopologyUtils.initialRanges(5, 500);
        Topology topology = TopologyUtils.initialTopology(ids, ranges, 3);
        Topology local = topology.forNode(ids.get(0));

        KeyRanges shards = CommandStores.shardRanges(local.getRanges(), 10).get(0);
        Assertions.assertEquals(ranges(r(0, 10), r(300, 310), r(400, 410)), shards);

        assertMapping(shards, shards(local, 0, 1, 2),
                      CommandStore.mapRanges(shards, local));
        assertMapping(ranges(r(0, 10), r(300, 310), r(390, 400), r(400, 410)), shards(local, 0, 1, 1, 2),
                      CommandStore.mapRanges(ranges(r(0, 10), r(300, 310), r(390, 410)), local));

        assertMapping(ranges(r(0, 10), r(300, 310), r(350, 360), r(400, 410)), shards(local, 0, 1, 1, 2),
                      CommandStore.mapRanges(ranges(r(0, 10), r(300, 310), r(350, 360), r(400, 410)), local));

    }
}
