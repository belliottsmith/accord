package accord.impl.list;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import accord.api.Key;
import accord.api.KeyRange;
import accord.api.Store;
import accord.api.Write;
import accord.topology.KeyRanges;
import accord.txn.Timestamp;
import accord.utils.Timestamped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListWrite extends TreeMap<Key, int[]> implements Write
{
    private static final Logger logger = LoggerFactory.getLogger(ListWrite.class);
    @Override
    public void apply(KeyRanges ranges, Timestamp executeAt, Store store)
    {
        ListStore s = (ListStore) store;
        for (KeyRange range : ranges)
        {
            NavigableMap<Key, int[]> selection = subMap(range.start(), range.startInclusive(),
                                                        range.end(), range.endInclusive());
            for (Map.Entry<Key, int[]> e : selection.entrySet())
            {
                Key key = e.getKey();
                int[] data = e.getValue();
                s.data.merge(key, new Timestamped<>(executeAt, data), Timestamped::merge);
                logger.trace("WRITE on {} at {} key:{} -> {}", s.node, executeAt, key, data);
            }
        }
    }
}
