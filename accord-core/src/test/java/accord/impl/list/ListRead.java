package accord.impl.list;

import accord.api.*;
<<<<<<< HEAD
=======
import accord.topology.KeyRange;
import accord.topology.KeyRanges;
>>>>>>> 9e2cbf0 (first draft)
import accord.txn.Keys;
import accord.txn.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ListRead implements Read
{
    private static final Logger logger = LoggerFactory.getLogger(ListRead.class);

    public final Keys keys;

    public ListRead(Keys keys)
    {
        this.keys = keys;
    }

    @Override
<<<<<<< HEAD
    public Data read(Key key, Timestamp executeAt, Store store)
=======
    public Data read(KeyRanges ranges, DataStore store)
>>>>>>> 9e2cbf0 (first draft)
    {
        ListStore s = (ListStore)store;
        ListData result = new ListData();
        int[] data = s.get(key);
        logger.trace("READ on {} at {} key:{} -> {}", s.node, executeAt, key, data);
        result.put(key, data);
        return result;
    }

    @Override
    public String toString()
    {
        return keys.toString();
    }
}
