package accord.local;

import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.Consumer;

import accord.api.Key;
import accord.api.Result;
import accord.local.Node.Id;
import accord.topology.KeyRanges;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;

import static accord.local.Status.Accepted;
import static accord.local.Status.Applied;
import static accord.local.Status.Committed;
import static accord.local.Status.Executed;
import static accord.local.Status.NotWitnessed;
import static accord.local.Status.PreAccepted;
import static accord.local.Status.ReadyToExecute;

// TODO: this needs to be backed by persistent storage
public class Command implements Listener, Consumer<Listener>
{
    public final CommandStore commandStore;
    private final TxnId txnId;
    private Key homeKey, someKey; // TODO: compress these
    private Txn txn; // TODO: only store this on the home shard, or split to each shard independently
    private Ballot promised = Ballot.ZERO, accepted = Ballot.ZERO;
    private Timestamp executeAt, executeAtExclusiveLowerBound; // TODO: compress these states together
    private Dependencies deps = new Dependencies();
    private Writes writes;
    private Result result;

    private Status status = NotWitnessed;
    private boolean isGloballyPersistent; // only set on home shard

    private NavigableMap<TxnId, Command> waitingOnCommit;
    private NavigableMap<Timestamp, Command> waitingOnApply;

    private final Listeners listeners = new Listeners();

    public Command(CommandStore commandStore, TxnId id)
    {
        this.commandStore = commandStore;
        this.txnId = id;
    }

    public TxnId txnId()
    {
        return txnId;
    }

    public Txn txn()
    {
        return txn;
    }

    public Ballot promised()
    {
        return promised;
    }

    public Ballot accepted()
    {
        return accepted;
    }

    public Timestamp executeAt()
    {
        return executeAt;
    }

    public Dependencies savedDeps()
    {
        return deps;
    }

    public Writes writes()
    {
        return writes;
    }

    public Result result()
    {
        return result;
    }

    public Status status()
    {
        return status;
    }

    public boolean hasBeen(Status status)
    {
        return this.status.compareTo(status) >= 0;
    }

    public boolean is(Status status)
    {
        return this.status == status;
    }

    public boolean isGloballyPersistent()
    {
        return isGloballyPersistent;
    }

    public void setGloballyPersistent()
    {
        isGloballyPersistent = true;
    }

    // requires that command != null
    // relies on mutual exclusion for each key
    // note: we do not set status = newStatus, we only use it to decide how we register with the retryLog
    private boolean witness(Txn txn, Key homeKey, boolean isHomeShard, Status newStatus)
    {
        if (newStatus.compareTo(Accepted) >= 0 && !hasBeen(Accepted) && !isHomeShard)
            commandStore.progressLog().nonHomePostPreaccept(txnId);

        if (promised.compareTo(Ballot.ZERO) > 0)
            return false;

        if (newStatus.compareTo(Committed) < 0 && !hasBeen(Committed) && isHomeShard)
            commandStore.progressLog().uncommitted(txnId);

        if (hasBeen(PreAccepted))
            return true;

        if (!isHomeShard && newStatus.compareTo(PreAccepted) <= 0)
            commandStore.progressLog().nonHomePreaccept(txnId);

        Timestamp max = commandStore.maxConflict(txn.keys);
        // unlike in the Accord paper, we partition shards within a node, so that to ensure a total order we must either:
        //  - use a global logical clock to issue new timestamps; or
        //  - assign each shard _and_ process a unique id, and use both as components of the timestamp
        Timestamp witnessed = txnId.compareTo(max) > 0 && txnId.epoch >= commandStore.latestEpoch() ? txnId : commandStore.uniqueNow(max);

        txn(txn);
        homeKey(homeKey);
        this.executeAt = witnessed;
        this.status = PreAccepted;

        txn.keys().forEach(key -> {
            if (commandStore.hashIntersects(key))
                commandStore.commandsForKey(key).register(this);
        });
        listeners.forEach(this);
        return true;
    }

    public boolean preaccept(Txn txn, Key homeKey)
    {
        boolean isHomeShard = isHomeShard(txnId.epoch, homeKey);
        return witness(txn, homeKey, isHomeShard, PreAccepted);
    }

    public boolean accept(Ballot ballot, Txn txn, Key homeKey, Timestamp executeAt, Dependencies deps)
    {
        if (this.promised.compareTo(ballot) > 0)
            return false;

        if (hasBeen(Committed))
            return false;

        boolean isHomeShard = isHomeShard(txnId.epoch, homeKey);
        witness(txn, homeKey, isHomeShard, Accepted);
        this.deps = deps;
        this.executeAt = executeAt;
        promised = accepted = ballot;
        status = Accepted;

        listeners.forEach(this);
        return true;
    }

    // relies on mutual exclusion for each key
    public boolean commit(Txn txn, Key homeKey, Dependencies deps, Timestamp executeAt)
    {
        if (hasBeen(Committed))
        {
            if (executeAt.equals(this.executeAt))
                return false;

            commandStore.agent().onInconsistentTimestamp(this, this.executeAt, executeAt);
        }

        boolean isHomeShard = isHomeShard(txnId.epoch, homeKey);
        witness(txn, homeKey, isHomeShard, Committed);
        this.status = Committed;
        this.deps = deps;
        this.executeAt = executeAt;
        this.waitingOnCommit = new TreeMap<>();
        this.waitingOnApply = new TreeMap<>();

        // TODO (now): we need to consider the epoch of the dependencies to decide if they're a dependency on this
        //             commandStore (and node), otherwise we may take a false dependency and wait for a transaction
        //             that won't be applied to us
        for (TxnId id : savedDeps().on(commandStore, executeAt))
        {
            Command command = commandStore.command(id);
            switch (command.status)
            {
                default:
                    throw new IllegalStateException();
                case NotWitnessed:
                    Txn depTxn = savedDeps().get(id);
                    command.someKey(depTxn.keys.get(0));
                    command.txn(depTxn);
                case PreAccepted:
                case Accepted:
                    // we don't know when these dependencies will execute, and cannot execute until we do
                    waitingOnCommit.put(id, command);
                    command.addListener(this);
                    break;
                case Committed:
                    // TODO: split into ReadyToRead and ReadyToWrite;
                    //       the distributed read can be performed as soon as those keys are ready, and in parallel with any other reads
                    //       the client can even ACK immediately after; only the write needs to be postponed until other in-progress reads complete
                case ReadyToExecute:
                case Executed:
                case Applied:
                    command.addListener(this);
                    updatePredecessor(command);
                    break;
            }
        }
        if (waitingOnCommit.isEmpty())
        {
            waitingOnCommit = null;
            if (waitingOnApply.isEmpty())
                waitingOnApply = null;
        }

        // TODO: we might not be the homeShard for later phases if we are no longer replicas of the range at executeAt;
        //       this should be fine, but it might be helpful to provide this info to the progressLog here?
        if (isHomeShard) commandStore.progressLog().committed(txnId);
        else commandStore.progressLog().nonHomeCommit(txnId);

        listeners.forEach(this);
        maybeExecute();
        return true;
    }

    public boolean apply(Txn txn, Key homeKey, Timestamp executeAt, Dependencies deps, Writes writes, Result result)
    {
        if (hasBeen(Executed) && executeAt.equals(this.executeAt))
            return false;
        else if (!hasBeen(Committed))
            commit(txn, homeKey, deps, executeAt);
        else if (!executeAt.equals(this.executeAt))
            commandStore.agent().onInconsistentTimestamp(this, this.executeAt, executeAt);

        this.executeAt = executeAt;
        this.writes = writes;
        this.result = result;
        this.status = Executed;

        this.commandStore.progressLog().executed(txnId);

        this.listeners.forEach(this);
        maybeExecute();
        return true;
    }

    public boolean recover(Txn txn, Key homeKey, Ballot ballot)
    {
        if (this.promised.compareTo(ballot) > 0)
            return false;

        boolean isHomeShard = isHomeShard(txnId.epoch, homeKey);
        witness(txn, homeKey, isHomeShard, PreAccepted);
        this.promised = ballot;
        return true;
    }

    public Command addListener(Listener listener)
    {
        listeners.add(listener);
        return this;
    }

    public void removeListener(Listener listener)
    {
        listeners.remove(listener);
    }

    @Override
    public void onChange(Command command)
    {
        switch (command.status)
        {
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
                if (command.executeAtExclusiveLowerBound == null || command.executeAtExclusiveLowerBound.compareTo(executeAt) <= 0)
                    break;

            case Committed:
            case ReadyToExecute:
            case Executed:
            case Applied:
                if (waitingOnApply != null)
                {
                    updatePredecessor(command);
                    if (waitingOnCommit != null)
                    {
                        if (waitingOnCommit.remove(command.txnId) != null && waitingOnCommit.isEmpty())
                            waitingOnCommit = null;
                    }
                    if (waitingOnCommit == null && waitingOnApply.isEmpty())
                        waitingOnApply = null;
                }
                else
                {
                    command.removeListener(this);
                }
                maybeExecute();
                break;
        }
    }

    private void maybeExecute()
    {
        if (status != Committed && status != Executed)
            return;

        if (waitingOnApply != null)
        {
            Command blockedBy = blockedBy();
            if (blockedBy != null)
            {
                commandStore.progressLog().waiting(txnId, blockedBy.txnId);
                return;
            }
            assert waitingOnApply == null;
        }

        switch (status)
        {
            case Committed:
                // TODO: maintain distinct ReadyToRead and ReadyToWrite states
                status = ReadyToExecute;
                if (isHomeShard(executeAt.epoch, homeKey)) // TODO (now): record distinct state if executeAt is not home shard?
                    commandStore.progressLog().readyToExecute(txnId);
                listeners.forEach(this);
                break;
            case Executed:
                writes.apply(commandStore);
                status = Applied;
                listeners.forEach(this);
        }
    }

    /**
     * @param dependency is either committed or has an executeAtExclusiveLowerBound >= our executeAt
     */
    private void updatePredecessor(Command dependency)
    {
        if (!dependency.hasBeen(Committed))
        {
            assert dependency.executeAtExclusiveLowerBound != null && dependency.executeAtExclusiveLowerBound.compareTo(executeAt) >= 0;
            dependency.removeListener(this);
        }
        else if (dependency.executeAt.compareTo(executeAt) > 0)
        {
            // cannot be a predecessor if we execute later
            dependency.removeListener(this);
        }
        else if (dependency.hasBeen(Applied))
        {
            waitingOnApply.remove(dependency.executeAt);
            dependency.removeListener(this);
        }
        else
        {
            waitingOnApply.putIfAbsent(dependency.executeAt, dependency);
        }
    }

    public Command blockedBy()
    {
        Command cur = directlyBlockedBy();
        if (cur == null)
            return null;

        Command next;
        while (null != (next = cur.directlyBlockedBy()))
            cur = next;
        return cur;
    }

    /**
     * A key nominated to represent the "home" shard - only members of the home shard may be nominated to recover
     * a transaction, to reduce the cluster-wide overhead of ensuring progress. A transaction that has only been
     * witnessed at PreAccept may however trigger a process of ensuring the home shard is durably informed of
     * the transaction.
     */
    public Key homeKey()
    {
        return homeKey;
    }

    public void homeKey(Key homeKey)
    {
        if (this.homeKey == null) this.homeKey = homeKey;
        else if (!this.homeKey.equals(homeKey)) throw new AssertionError();
    }

    public Key someKey()
    {
        return homeKey != null ? homeKey : someKey;
    }

    private void someKey(Key key)
    {
        if (someKey == null && homeKey == null)
            someKey = key;
    }

    public void txn(Txn txn)
    {
        if (this.txn == null) this.txn = txn;
        else if (!this.txn.equals(txn)) throw new AssertionError();
    }

    // TODO: maybe make this persistent, or abstract so implementation may do so
    public void mustExecuteAfter(Timestamp exclusiveLowerBound)
    {
        if (executeAtExclusiveLowerBound == null || executeAtExclusiveLowerBound.compareTo(exclusiveLowerBound) < 0)
        {
            executeAtExclusiveLowerBound = exclusiveLowerBound;
            listeners.forEach(this);
        }
    }

    private long homeEpoch()
    {
        switch (status)
        {
            default: throw new AssertionError();
            case NotWitnessed:
            case PreAccepted:
            case Accepted:
            case Committed:
                return txnId.epoch;
            case ReadyToExecute:
            case Executed:
            case Applied:
                return executeAt.epoch;
        }
    }

    private boolean isHomeShard(long epoch, Key homeKey)
    {
        KeyRanges ranges = commandStore.ranges(epoch);
        if (ranges == null)
            return false;
        return ranges.contains(homeKey);
    }

    private Id coordinator()
    {
        if (promised.equals(Ballot.ZERO))
            return txnId.node;
        return promised.node;
    }

    private Command directlyBlockedBy()
    {
        // firstly we're waiting on every dep to commit
        while (waitingOnCommit != null)
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            Command waitingOn = waitingOnCommit.firstEntry().getValue();
            if (!waitingOn.hasBeen(Committed)) return waitingOn;
            onChange(waitingOn);
        }

        while (waitingOnApply != null)
        {
            // TODO: when we change our liveness mechanism this may not be a problem
            // cannot guarantee that listener updating this set is invoked before this method by another listener
            // so we must check the entry is still valid, and potentially remove it if not
            Command waitingOn = waitingOnApply.firstEntry().getValue();
            if (!waitingOn.hasBeen(Applied)) return waitingOn;
            onChange(waitingOn);
        }

        return null;
    }

    @Override
    public void accept(Listener listener)
    {
        listener.onChange(this);
    }

    @Override
    public String toString()
    {
        return "Command{" +
               "txnId=" + txnId +
               ", status=" + status +
               ", txn=" + txn +
               ", executeAt=" + executeAt +
               ", deps=" + deps +
               '}';
    }
}
