package accord.messages;

import accord.local.Node;
import accord.topology.KeyRanges;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.txn.Keys;
import accord.txn.Txn;

import java.util.Objects;

import com.google.common.base.Preconditions;

public abstract class TxnRequest implements Request
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

    /**
     * Indicates the keys the coordinator expects the recipient to service for a request, and
     * the minimum epochs the recipient will need to be aware of for each set of keys
     */
    public static class Scope
    {
        private final long evaluationEpoch;
        private final long minRequiredEpoch;
        private final Keys keys;

        public Scope(long evaluationEpoch, long minRequiredEpoch, Keys keys)
        {
            this.evaluationEpoch = evaluationEpoch;
            Preconditions.checkArgument(!keys.isEmpty());
            this.minRequiredEpoch = minRequiredEpoch;
            this.keys = keys;
        }

        public static Scope forTopologies(Node.Id node, Topologies topologies, Keys keys, long evaluationEpoch)
        {
            long minEpoch = 0;
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
                    minEpoch = topology.epoch();
                    scopeKeys = scopeKeys.union(epochKeys);
                }
                lastKeys = epochKeys;
            }

            return new Scope(evaluationEpoch, minEpoch, scopeKeys);
        }

        public static Scope forTopologies(Node.Id node, Topologies topologies, Txn txn, long evaluationEpoch)
        {
            return forTopologies(node, topologies, txn.keys(), evaluationEpoch);
        }

        public long minRequiredEpoch()
        {
            return minRequiredEpoch;
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
            return minRequiredEpoch == scope.minRequiredEpoch && keys.equals(scope.keys);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(minRequiredEpoch, keys);
        }

        @Override
        public String toString()
        {
            return "Scope{" +
                    "maxEpoch=" + minRequiredEpoch +
                    ", keys=" + keys +
                    '}';
        }
    }
}
