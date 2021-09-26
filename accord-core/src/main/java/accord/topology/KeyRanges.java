package accord.topology;

import accord.api.Key;
import accord.api.KeyRange;
import accord.txn.Keys;
import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class KeyRanges implements Iterable<KeyRange>
{
    public static final KeyRanges EMPTY = new KeyRanges(new KeyRange[0]);

    // TODO: fix raw parameterized use
    private final KeyRange[] ranges;

    public KeyRanges(KeyRange[] ranges)
    {
        this.ranges = ranges;
    }

    @Override
    public String toString()
    {
        return Arrays.toString(ranges);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyRanges ranges1 = (KeyRanges) o;
        return Arrays.equals(ranges, ranges1.ranges);
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode(ranges);
    }

    @Override
    public Iterator<KeyRange> iterator()
    {
        return Iterators.forArray(ranges);
    }

    public int rangeIndexForKey(int lowerBound, int upperBound, Key key)
    {
        return Arrays.binarySearch(ranges, lowerBound, upperBound, key,
                                   (r, k) -> -((KeyRange) r).compareKey((Key) k));
    }

    public int rangeIndexForKey(Key key)
    {
        return rangeIndexForKey(0, ranges.length, key);
    }

    public int size()
    {
        return ranges.length;
    }

    public KeyRanges select(int[] indexes)
    {
        KeyRange[] selection = new KeyRange[indexes.length];
        for (int i=0; i<indexes.length; i++)
            selection[i] = ranges[i];
        return new KeyRanges(selection);
    }

    public boolean intersects(Keys keys)
    {
        for (int i=0; i<ranges.length; i++)
            if (ranges[i].intersects(keys))
                return true;
        return false;
    }

    /**
     * Subtracts the given set of key ranges from this
     * @param that
     * @return
     */
    public KeyRanges difference(KeyRanges that)
    {
        List<KeyRange> result = new ArrayList<>(this.size() + that.size());
        int thatIdx = 0;

        for (int thisIdx=0; thisIdx<this.size(); thisIdx++)
        {
            KeyRange thisRange = this.ranges[thisIdx];
            while (thatIdx < that.size())
            {
                KeyRange thatRange = that.ranges[thatIdx];

                int cmp = thisRange.compareIntersecting(thatRange);
                if (cmp < 0)
                {
                    thatIdx++;
                    continue;
                }
                if (cmp > 0) break;

                int scmp = thisRange.start().compareTo(thatRange.start());
                int ecmp = thisRange.end().compareTo(thatRange.end());

                if (scmp < 0)
                    result.add(thisRange.subRange(thisRange.start(), thatRange.start()));

                if (ecmp <= 0)
                {
                    thisRange = null;
                    break;
                }
                else
                {
                    thisRange = thisRange.subRange(thatRange.end(), thisRange.end());
                    thatIdx++;
                }
            }
            if (thisRange != null)
                result.add(thisRange);
        }
        return new KeyRanges(result.toArray(KeyRange[]::new));
    }

}
