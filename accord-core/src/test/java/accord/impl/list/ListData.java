package accord.impl.list;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

import accord.api.Data;
import accord.api.Key;

public class ListData extends TreeMap<Key, int[]> implements Data
{
    @Override
    public Data merge(Data data)
    {
        if (data != null)
            this.putAll(((ListData)data));
        return this;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder().append('{');
        boolean first = true;
        for (Map.Entry<Key, int[]> entry : entrySet())
        {
            if (!first)
                sb.append(", ");
            sb.append(entry.getKey()).append(" -> ").append(Arrays.toString(entry.getValue()));
            first = false;
        }
        return sb.append('}').toString();
    }
}
