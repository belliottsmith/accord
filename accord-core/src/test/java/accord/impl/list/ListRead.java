package accord.impl.list;

import accord.api.*;
import accord.topology.KeyRanges;
import accord.txn.Keys;
import accord.txn.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.max;

public class ListRead implements Read
{
    private static final Logger logger = LoggerFactory.getLogger(ListRead.class);

    public final Keys keys;

    public ListRead(Keys keys)
    {
        this.keys = keys;
    }

    @Override
    public Data read(KeyRanges ranges, Timestamp executeAt, Store store)
    {
        ListStore s = (ListStore)store;
        ListData result = new ListData();
        for (KeyRange range : ranges)
        {
            int lowIdx = range.lowKeyIndex(keys);
            if (lowIdx < -keys.size())
                return result;
            if (lowIdx < 0)
                continue;
            for (int i = lowIdx, limit = range.higherKeyIndex(keys) ; i < limit ; ++i)
            {
                Key key = keys.get(i);
                int[] data = s.get(key);
                logger.trace("READ on {} key:{} -> {}", s.node, key, data);
                result.put(key, data);
            }
        }
        return result;
    }

    @Override
    public String toString()
    {
        return keys.toString();
    }
}
