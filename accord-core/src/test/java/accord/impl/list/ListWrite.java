package accord.impl.list;

import java.util.Arrays;
import java.util.TreeMap;
import java.util.stream.Collectors;

import accord.api.Key;
<<<<<<< HEAD
import accord.api.Store;
=======
import accord.topology.KeyRange;
import accord.api.DataStore;
>>>>>>> 9e2cbf0 (first draft)
import accord.api.Write;
import accord.txn.Timestamp;
import accord.utils.Timestamped;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListWrite extends TreeMap<Key, int[]> implements Write
{
    private static final Logger logger = LoggerFactory.getLogger(ListWrite.class);
    @Override
<<<<<<< HEAD
    public void apply(Key key, Timestamp executeAt, Store store)
=======
    public void apply(KeyRanges ranges, Timestamp executeAt, DataStore store)
>>>>>>> 9e2cbf0 (first draft)
    {
        ListStore s = (ListStore) store;
        if (!containsKey(key))
            return;
        int[] data = get(key);
        s.data.merge(key, new Timestamped<>(executeAt, data), Timestamped::merge);
        logger.trace("WRITE on {} at {} key:{} -> {}", s.node, executeAt, key, data);
    }

    @Override
    public String toString()
    {
        return entrySet().stream()
                         .map(e -> e.getKey() + "=" + Arrays.toString(e.getValue()))
                         .collect(Collectors.joining(", ", "{", "}"));
    }
}
