package accord.impl.list;

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

public class ListWrite extends TreeMap<Key, int[]> implements Write
{
    @Override
    public void apply(KeyRanges ranges, Timestamp executeAt, DataStore store)
    {
        ListStore s = (ListStore) store;
        for (KeyRange range : ranges)
        {
            NavigableMap<Key, int[]> selection = subMap(range.start(), range.startInclusive(),
                                                        range.end(), range.endInclusive());
            for (Map.Entry<Key, int[]> e : selection.entrySet())
                s.data.merge(e.getKey(), new Timestamped<>(executeAt, e.getValue()), Timestamped::merge);
        }
    }
}
