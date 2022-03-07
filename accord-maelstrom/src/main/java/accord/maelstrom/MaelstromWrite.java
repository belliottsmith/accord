package accord.maelstrom;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import accord.api.Key;
import accord.topology.KeyRange;
import accord.api.DataStore;
import accord.api.Write;
import accord.topology.KeyRanges;
import accord.txn.Timestamp;
import accord.utils.Timestamped;

public class MaelstromWrite extends TreeMap<Key, Value> implements Write
{
    @Override
    public void apply(KeyRanges ranges, Timestamp executeAt, DataStore store)
    {
        MaelstromStore s = (MaelstromStore) store;
        for (KeyRange range : ranges)
        {
            NavigableMap<Key, Value> selection = subMap(range.start(), range.startInclusive(),
                                                        range.end(), range.endInclusive());
            for (Map.Entry<Key, Value> e : selection.entrySet())
                s.data.merge(e.getKey(), new Timestamped<>(executeAt, e.getValue()), Timestamped::merge);
        }
    }
}
