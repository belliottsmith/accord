package accord.messages;

import accord.local.Node;
import accord.topology.KeyRanges;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Txn;

import java.util.Objects;

import com.google.common.base.Preconditions;

public abstract class TxnRequest implements EpochRequest
{
    private final Scope scope;

    public TxnRequest(Scope scope)
    {
        this.scope = scope;
    }

    public Scope scope()
    {
        return scope;
    }

    public long waitForEpoch()
    {
        return scope().waitForEpoch();
    }

    /**
     * Indicates the keys the coordinator expects the recipient to service for a request, and
     * the minimum epochs the recipient will need to be aware of for each set of keys
     */
    public static class Scope
    {
        // the first epoch we should process
        private final long minEpoch;

        // the last epoch on which this command must be logically processed,
        // i.e. for Accept and Commit this is executeAt.epoch
        //      for all other commands this is the same as minEpoch
        private final long maxEpoch;

        // the epoch we must wait for to be able to safely process this operation
        private final long waitForEpoch;
        private final Keys keys;

        public Scope(long minEpoch, long maxEpoch, long waitForEpoch, Keys keys)
        {
            this.minEpoch = minEpoch;
            this.maxEpoch = maxEpoch;
            Preconditions.checkArgument(!keys.isEmpty());
            this.waitForEpoch = waitForEpoch;
            this.keys = keys;
        }

        public static Scope forTopologies(Node.Id node, Topologies topologies, Keys keys)
        {
            long waitForEpoch = 0;
            Keys scopeKeys = Keys.EMPTY;
            Keys lastKeys = null;
            for (int i=topologies.size() - 1; i>=0; i--)
            {
                Topology topology = topologies.get(i);
                KeyRanges topologyRanges = topology.rangesForNode(node);
                if (topologyRanges == null)
                    continue;
                topologyRanges = topologyRanges.intersection(keys);
                Keys epochKeys = keys.intersect(topologyRanges);
                if (lastKeys == null || !lastKeys.equals(epochKeys))
                {
                    waitForEpoch = topology.epoch();
                    scopeKeys = scopeKeys.union(epochKeys);
                }
                lastKeys = epochKeys;
            }

            return new Scope(topologies.oldestEpoch(), topologies.currentEpoch(), waitForEpoch, scopeKeys);
        }

        public static Scope forTopologies(Node.Id node, Topologies topologies, Txn txn)
        {
            return forTopologies(node, topologies, txn.keys());
        }

        public long minEpoch()
        {
            return minEpoch;
        }

        public long maxEpoch()
        {
            return maxEpoch;
        }

        public long waitForEpoch()
        {
            return waitForEpoch;
        }

        public Keys keys()
        {
            return keys;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Scope scope = (Scope) o;
            return waitForEpoch == scope.waitForEpoch
                   && minEpoch == scope.minEpoch
                   && maxEpoch == scope.maxEpoch
                   && keys.equals(scope.keys);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(minEpoch, maxEpoch, waitForEpoch, keys);
        }

        @Override
        public String toString()
        {
            return "Scope{" +
                   "minEpoch=" + minEpoch +
                   ", maxEpoch=" + maxEpoch +
                   ", waitForEpoch=" + waitForEpoch +
                   ", keys=" + keys +
                   '}';
        }
    }
}
