package accord.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.coordinate.CheckOnCommitted;
import accord.coordinate.CheckOnUncommitted;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.Apply;
import accord.messages.Callback;
import accord.messages.CheckStatus;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.MessageType;
import accord.messages.Reply;
import accord.messages.ReplyContext;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;

import static accord.coordinate.CheckOnCommitted.checkOnCommitted;
import static accord.coordinate.CheckOnUncommitted.checkOnUncommitted;
import static accord.coordinate.InformHomeOfTxn.inform;
import static accord.coordinate.MaybeRecover.maybeRecover;
import static accord.impl.SimpleProgressLog.CoordinateApplyAndCheck.applyAndCheck;
import static accord.impl.SimpleProgressLog.ExternalStatus.Committed;
import static accord.impl.SimpleProgressLog.ExternalStatus.ExecutedOnAllShardsButNotThisInstance;
import static accord.impl.SimpleProgressLog.ExternalStatus.NonHomePreAccept;
import static accord.impl.SimpleProgressLog.ExternalStatus.NonHomeSafe;
import static accord.impl.SimpleProgressLog.ExternalStatus.NonHomeWaitingOnExecute;
import static accord.impl.SimpleProgressLog.ExternalStatus.NonHomeWaitingOnCommit;
import static accord.impl.SimpleProgressLog.ExternalStatus.None;
import static accord.impl.SimpleProgressLog.ExternalStatus.ReadyToExecute;
import static accord.impl.SimpleProgressLog.ExternalStatus.ExecutedOnAllShards;
import static accord.impl.SimpleProgressLog.ExternalStatus.Uncommitted;
import static accord.impl.SimpleProgressLog.ExternalStatus.ExecutedAndWaitingForAllShards;
import static accord.impl.SimpleProgressLog.HomeState.GlobalStatus.Durable;
import static accord.impl.SimpleProgressLog.HomeState.GlobalStatus.InProgress;
import static accord.impl.SimpleProgressLog.HomeState.GlobalStatus.NotExecuted;
import static accord.impl.SimpleProgressLog.HomeState.LocalStatus.NotWitnessed;
import static accord.impl.SimpleProgressLog.InternalStatus.Done;
import static accord.impl.SimpleProgressLog.InternalStatus.Investigating;
import static accord.impl.SimpleProgressLog.InternalStatus.NoProgress;
import static accord.impl.SimpleProgressLog.InternalStatus.NoProgressExpected;
import static accord.impl.SimpleProgressLog.InternalStatus.Waiting;
import static accord.impl.SimpleProgressLog.Progress.Expected;
import static accord.impl.SimpleProgressLog.Progress.NoneExpected;
import static accord.local.Status.Executed;
import static accord.local.Status.PreAccepted;

public class SimpleProgressLog implements Runnable, ProgressLog.Factory
{
    enum ExternalStatus
    {
        None, NonHomePreAccept, NonHomeSafe, NonHomeWaitingOnCommit, NonHomeWaitingOnExecute, // TODO: WaitingOn is only state taken in non-progressKey log; make a parallel state?
        Uncommitted, Committed, ReadyToExecute,
        ExecutedAndWaitingForAllShards, ExecutedOnAllShardsButNotThisInstance, ExecutedOnAllShards
    }

    enum ParallelStatus { WaitingOnCommit, WaitingOnExecute, PersistedGlobally }

    enum InternalStatus { Waiting, NoProgress, Investigating, NoProgressExpected, Done }

    enum Progress
    {
        Expected, NotWitnessed, Investigating, NoneExpected, Done
    }

    static class HomeState
    {
        enum LocalStatus
        {
            NotWitnessed, Uncommitted, Committed, ReadyToExecute, Done;
        }

        enum GlobalStatus
        {
            NotExecuted, InProgress, Durable, Done
        }

        LocalStatus local = NotWitnessed;
        Progress localProgress = NoneExpected;
        GlobalStatus global = NotExecuted;
        Progress globalProgress = NoneExpected;
        Set<Id> notPersisted;

        Status maxStatus;
        Ballot maxPromised;
        boolean maxPromiseHasBeenAccepted;

        void updateMax(Command command)
        {
            if (maxStatus == null || maxStatus.compareTo(command.status()) < 0)
                maxStatus = command.status();
            if (maxPromised == null || maxPromised.compareTo(command.promised()) < 0)
                maxPromised = command.promised();
            maxPromiseHasBeenAccepted |= command.accepted().equals(maxPromised);
        }

        void updateMax(CheckStatusOk ok)
        {
            // TODO: perhaps set localProgress back to Waiting if Investigating and we update anything?
            if (ok.status.compareTo(maxStatus) > 0) maxStatus = ok.status;
            if (ok.promised.compareTo(maxPromised) > 0)
            {
                maxPromised = ok.promised;
                maxPromiseHasBeenAccepted = ok.accepted.equals(ok.promised);
            }
            else if (ok.promised.equals(maxPromised))
            {
                maxPromiseHasBeenAccepted |= ok.accepted.equals(ok.promised);
            }
        }

        private void initialiseGlobal(Node node, Command command)
        {
            if (global == NotExecuted)
            {
                notPersisted = new HashSet<>(node.topology().unsyncForTxn(command.txn(), command.executeAt().epoch).nodes());
                notPersisted.remove(node.id());
                global = InProgress;
                globalProgress = Expected;
            }
        }

        private void refreshGlobal()
        {
            if (notPersisted.isEmpty())
            {
                global = GlobalStatus.Done;
                globalProgress = NoneExpected;
            }
        }

        void executedOnAllShards(Node node, Command command, Set<Id> persistedOn)
        {
            initialiseGlobal(node, command);
            notPersisted.removeAll(persistedOn);
            if (global.compareTo(Durable) < 0)
                global = Durable;
            refreshGlobal();
        }

        void executed(Node node, Command command)
        {
            switch (local)
            {
                default: throw new IllegalStateException();
                case NotWitnessed:
                case Uncommitted:
                case Committed:
                case ReadyToExecute:
                    local = LocalStatus.Done;
                    localProgress = NoneExpected;
                    initialiseGlobal(node, command);
                    notPersisted.remove(node.id());
                    refreshGlobal();
                case Done:
            }
        }

        @Override
        public String toString()
        {
            return "{" + local + ',' + localProgress + ',' + global + ',' + globalProgress + '}';
        }

    }

    static class BlockingState
    {
        Timestamp blockedOnCommit;
        Timestamp blockedOnExecute;
        boolean investigating;

        void record(Timestamp waitingAt, Command blockedBy)
        {
            if (blockedBy.is(Status.Committed))
            {
                if (blockedOnExecute == null || blockedOnExecute.compareTo(waitingAt) < 0)
                    blockedOnExecute = waitingAt;
                // we don't need to reset investigating state in this case, as we can execute for future epochs on arrival
            }
            else
            {
                if (blockedOnCommit == null || blockedOnCommit.compareTo(waitingAt) < 0)
                {
                    blockedOnCommit = waitingAt;
                    // we reset investigating state as the timestamp is not a lower bound for the blocking task's execution
                    // until we have contacted a quorum
                    investigating = false;
                }
            }
        }
    }

    enum NonHomeState
    {
        Unsafe, Investigating, Safe
    }

    static class NewState
    {
        final TxnId txnId;
        final Command command;

        HomeState homeState;
        NonHomeState nonHomeState;
        BlockingState blockingState;

        Object debugInProgress;

        NewState(TxnId txnId, Command command)
        {
            this.txnId = txnId;
            this.command = command;
        }

        void recordBlocker(Command waitingCommand, Command blockedByCommand)
        {
            Preconditions.checkArgument(blockedByCommand.txnId().equals(txnId));
            if (blockingState == null)
                blockingState = new BlockingState();
            blockingState.record(waitingCommand.executeAt(), blockedByCommand);
        }
    }

    static class State
    {
        final TxnId txnId;

        Command command;
        ExternalStatus externalStatus = None;
        InternalStatus internalStatus = NoProgressExpected;
        Set<Id> waitingOnExecute; // TODO: limit to the shards owned by the node

        // this includes state witnessed from other nodes
        Status maxStatus;
        Ballot maxPromised;
        boolean maxPromiseHasBeenAccepted;

        Timestamp blockingTxnWithExecuteAt;

        Object debugInProgress;

        State(TxnId txnId)
        {
            this.txnId = txnId;
        }

        State ensureAtLeast(ExternalStatus newExternalStatus, InternalStatus newInternalStatus)
        {
            return ensureAtLeast(newExternalStatus, newInternalStatus, null);
        }

        State ensureAtLeast(ExternalStatus newExternalStatus, InternalStatus newInternalStatus, Command newCommand)
        {
            if (externalStatus == null || externalStatus.compareTo(newExternalStatus) < 0)
            {
                externalStatus = newExternalStatus;
                internalStatus = newInternalStatus;
                if (newCommand != null)
                    setCommand(newCommand);
            }
            return this;
        }

        void setCommand(Command command)
        {
            if (this.command == null) this.command = command;
            else Preconditions.checkState(command == this.command);

            if (maxStatus == null || maxStatus.compareTo(command.status()) < 0)
                maxStatus = command.status();
            if (maxPromised == null || maxPromised.compareTo(command.promised()) < 0)
                maxPromised = command.promised();
            maxPromiseHasBeenAccepted |= command.accepted().equals(maxPromised);
        }

        void recordBlocker(Command waitingCommand, Command blockedByUncommittedCommand)
        {
            Preconditions.checkArgument(blockedByUncommittedCommand.txnId().equals(txnId));
            if (blockingTxnWithExecuteAt == null || blockingTxnWithExecuteAt.compareTo(waitingCommand.executeAt()) < 0)
            {
                blockingTxnWithExecuteAt = waitingCommand.executeAt();
            }
        }

        void recordForwardProgress(CheckStatusOk ok)
        {
            if (ok.status.compareTo(maxStatus) > 0) maxStatus = ok.status;
            if (ok.promised.compareTo(maxPromised) > 0)
            {
                maxPromised = ok.promised;
                maxPromiseHasBeenAccepted = ok.accepted.equals(ok.promised);
            }
            else if (ok.promised.equals(maxPromised))
            {
                maxPromiseHasBeenAccepted |= ok.accepted.equals(ok.promised);
            }
        }

        void executedOnAllShards(Command newCommand)
        {
            if (newCommand != null)
                setCommand(newCommand);

            waitingOnExecute = Collections.emptySet();
            if (externalStatus.compareTo(ExecutedAndWaitingForAllShards) < 0 && command.executes())
            {
                externalStatus = ExecutedOnAllShardsButNotThisInstance;
                internalStatus = NoProgress;
            }
            else
            {
                externalStatus = ExecutedOnAllShards;
                internalStatus = NoProgressExpected;
            }
        }

        void executed(Node node, Command command, boolean isHomeShard)
        {
            setCommand(command);

            switch (externalStatus)
            {
                default: throw new IllegalStateException();
                case None:
                case NonHomeWaitingOnCommit:
                case NonHomeWaitingOnExecute:
                case NonHomePreAccept:
                case NonHomeSafe:
                case Uncommitted:
                case Committed:
                case ReadyToExecute:
                    if (isHomeShard)
                    {
                        waitingOnExecute = new HashSet<>(node.topology().unsyncForTxn(this.command.txn(), this.command.executeAt().epoch).nodes());
                        waitingOnExecute.remove(node.id());
                        externalStatus = ExecutedAndWaitingForAllShards;
                        break;
                    }
                case ExecutedOnAllShardsButNotThisInstance:
                    waitingOnExecute = Collections.emptySet();
                    externalStatus = ExecutedOnAllShards;
                    internalStatus = NoProgressExpected;
                case ExecutedAndWaitingForAllShards:
                case ExecutedOnAllShards:
            }
        }

        @Override
        public String toString()
        {
            return "{" + externalStatus + ',' + internalStatus + '}';
        }
    }

    final Node node;
    final List<Instance> instances = new CopyOnWriteArrayList<>();

    public SimpleProgressLog(Node node)
    {
        this.node = node;
        node.scheduler().recurring(this, 200L, TimeUnit.MILLISECONDS);
    }

    class Instance implements ProgressLog
    {
        final CommandStore commandStore;
        final Map<TxnId, State> stateMap = new HashMap<>();

        Instance(CommandStore commandStore)
        {
            this.commandStore = commandStore;
            instances.add(this);
        }

        private void nonHomeUnsafe(TxnId txnId)
        {
            Command command = commandStore.command(txnId);
            stateMap.computeIfAbsent(txnId, State::new)
                    .ensureAtLeast(NonHomePreAccept, Waiting, command);
        }

        private void nonHomeSafe(TxnId txnId)
        {
            Command command = commandStore.command(txnId);
            stateMap.computeIfAbsent(txnId, State::new)
                    .ensureAtLeast(NonHomeSafe, NoProgressExpected, command);
        }

        private void nonHomeCommit(TxnId txnId)
        {
            State state = stateMap.get(txnId);
            if (state != null && state.externalStatus == NonHomeWaitingOnCommit)
            {
                state.externalStatus = NonHomeSafe;
                state.internalStatus = NoProgressExpected;
            }
        }

        private void homeUncommitted(TxnId txnId)
        {
            Command command = commandStore.command(txnId);
            stateMap.computeIfAbsent(txnId, State::new)
                    .ensureAtLeast(Uncommitted, Waiting, command);
        }

        @Override
        public void preaccept(TxnId txnId, boolean isHomeShard)
        {
            if (isHomeShard) homeUncommitted(txnId);
            else nonHomeUnsafe(txnId);
        }

        @Override
        public void accept(TxnId txnId, boolean isHomeShard)
        {
            if (isHomeShard) homeUncommitted(txnId);
            else nonHomeSafe(txnId);
        }

        @Override
        public void commit(TxnId txnId, boolean isHomeCommitShard)
        {
            if (!isHomeCommitShard)
            {
                nonHomeCommit(txnId);
            }
            else
            {
                Command command = commandStore.command(txnId);
                stateMap.computeIfAbsent(txnId, State::new)
                        .ensureAtLeast(Committed, NoProgressExpected, command);
            }
        }

        @Override
        public void readyToExecute(TxnId txnId, boolean isHomeShard)
        {
            if (!isHomeShard)
                return;

            Command command = commandStore.command(txnId);
            stateMap.computeIfAbsent(txnId, State::new)
                    .ensureAtLeast(ReadyToExecute, Waiting, command);
        }

        @Override
        public void executed(TxnId txnId, boolean isHomeShard)
        {
            Command command = commandStore.command(txnId);
            stateMap.computeIfAbsent(txnId, State::new)
                    .executed(node, command, isHomeShard);
        }

        @Override
        public void executedOnAllShards(TxnId txnId)
        {
            Command command = commandStore.command(txnId);
            stateMap.computeIfAbsent(txnId, State::new)
                    .executedOnAllShards(command);
        }

        @Override
        public void waiting(TxnId waiting, TxnId blockedBy)
        {
            Command waitingCommand = commandStore.command(waiting);
            Command blockedByCommand = commandStore.command(blockedBy);
            // TODO: sync should probably wait until commands are executed and forward them on to future owners,
            //       or at least recipients should request data as part of building once the set of commands is known;
            //       in which case this can instead be
            //       if (!blockedByCommand.hasBeen(Committed))
            if (!blockedByCommand.hasBeen(Status.Committed))
            {
                stateMap.computeIfAbsent(blockedBy, State::new)
                        .ensureAtLeast(NonHomeWaitingOnCommit, Waiting, blockedByCommand)
                        .recordBlocker(waitingCommand, blockedByCommand);
            }
            else if (!blockedByCommand.hasBeen(Executed))
            {
                stateMap.computeIfAbsent(blockedBy, State::new)
                        .ensureAtLeast(NonHomeWaitingOnExecute, Waiting, blockedByCommand)
                        .recordBlocker(waitingCommand, blockedByCommand);
            }
        }

        SimpleProgressLog parent()
        {
            return SimpleProgressLog.this;
        }
    }

    @Override
    public void run()
    {
        for (Instance instance : instances)
        {
            for (Map.Entry<TxnId, State> entry : instance.stateMap.entrySet())
            {
                TxnId txnId = entry.getKey();
                State state = entry.getValue();
                switch (state.internalStatus)
                {
                    default: throw new AssertionError();
                    case Waiting:
                        state.internalStatus = NoProgress;
                    case NoProgressExpected:
                    case Investigating:
                    case Done:
                        continue;
                    case NoProgress:
                }

                // TODO (now): we need to be able to poll others about pending dependencies to check forward progress,
                //             as we don't know all dependencies locally (or perhaps any, at execution time)
                state.internalStatus = Investigating;
                switch (state.externalStatus)
                {
                    default:
                    case ExecutedOnAllShards:
                        throw new AssertionError();

                    case NonHomePreAccept:
                    {
                        // make sure a quorum of the home shard is aware of the transaction, so we can rely on it to ensure progress
                        CompletionStage<Void> inform = inform(node, txnId, state.command.txn(), state.command.homeKey());
                        state.debugInProgress = inform;
                        inform.whenComplete((success, fail) -> {
                            if (state.externalStatus != NonHomePreAccept)
                                return;

                            if (fail != null) state.internalStatus = NoProgress;
                            else if (state.internalStatus == Investigating) state.internalStatus = NoProgressExpected;
                        });
                        break;
                    }

                    case NonHomeWaitingOnCommit:
                    {
                        // check status with the only keys we know, if any, then:
                        // 1. if we cannot find any primary record of the transaction, then it cannot be a dependency so record this fact
                        // 2. otherwise record the homeKey for future reference and set the status based on whether progress has been made
                        Timestamp maxExecuteAtWithTxnAsDependency = state.blockingTxnWithExecuteAt;
                        Key someKey = state.command.someKey();
                        Shard someShard = node.topology().forEpochIfKnown(someKey, txnId.epoch);
                        if (someShard == null)
                        {
                            node.configService().fetchTopologyForEpoch(txnId.epoch);
                            state.internalStatus = Waiting;
                            continue;
                        }
                        // TODO (now): if we have a quorum of PreAccept and we have contacted the home shard then we can set NonHomeSafe
                        CheckOnUncommitted check = checkOnUncommitted(node, txnId, state.command.txn(),
                                                                      someKey, someShard, txnId.epoch,
                                                                      state.blockingTxnWithExecuteAt);
                        state.debugInProgress = check;
                        check.whenComplete((success, fail) -> {
                            if (state.externalStatus != NonHomeWaitingOnCommit)
                                return;

                            // set the new status immediately to account for exceptions;
                            // we retry at the next interval in all cases as this operation is non-competitive
                            state.internalStatus = NoProgress;
                            if (fail != null) // TODO: log?
                                return;

                            if ((success.status.compareTo(PreAccepted) < 0 && state.blockingTxnWithExecuteAt.equals(maxExecuteAtWithTxnAsDependency))
                                || success.status.compareTo(Status.Committed) >= 0)
                            {
                                state.externalStatus = NonHomeSafe;
                                state.internalStatus = NoProgressExpected;
                            }
                        });
                        break;
                    }

                    case NonHomeWaitingOnExecute:
                    {
                        Key homeKey = state.command.homeKey();
                        long homeEpoch = state.command.executeAt().epoch;
                        Shard homeShard = node.topology().forEpochIfKnown(homeKey, homeEpoch);
                        if (homeShard == null)
                        {
                            node.configService().fetchTopologyForEpoch(txnId.epoch);
                            state.internalStatus = Waiting;
                            continue;
                        }
                        // TODO (now): if we have a quorum of PreAccept and we have contacted the home shard then we can set NonHomeSafe
                        CheckOnCommitted check = checkOnCommitted(node, txnId, state.command.txn(),
                                                                  homeKey, homeShard, homeEpoch,
                                                                  state.blockingTxnWithExecuteAt);
                        state.debugInProgress = check;
                        check.whenComplete((success, fail) -> {
                            if (state.externalStatus != NonHomeWaitingOnExecute)
                                return;

                            // set the new status immediately to account for exceptions;
                            // we retry at the next interval in all cases as this operation is non-competitive
                            state.internalStatus = NoProgress;
                            if (fail != null) // TODO: log?
                                return;

                            if (success.status.compareTo(Executed) >= 0)
                            {
                                state.externalStatus = NonHomeSafe;
                                state.internalStatus = NoProgressExpected;
                            }
                        });
                        break;
                    }

                    case Uncommitted:
                    case ReadyToExecute:
                    {
                        Key homeKey = state.command.homeKey();
                        long homeEpoch = (state.externalStatus.compareTo(Uncommitted) <= 0 ? txnId : state.command.executeAt()).epoch;
                        Shard homeShard = node.topology().forEpochIfKnown(homeKey, homeEpoch);
                        if (homeShard == null)
                        {
                            node.configService().fetchTopologyForEpoch(txnId.epoch);
                            state.internalStatus = Waiting;
                            continue;
                        }
                        CompletionStage<CheckStatusOk> recover = maybeRecover(node, txnId, state.command.txn(),
                                                                              homeKey, homeShard, homeEpoch,
                                                                              state.maxStatus, state.maxPromised, state.maxPromiseHasBeenAccepted);
                        state.debugInProgress = recover;
                        recover.whenComplete((success, fail) -> {
                            if (state.externalStatus.compareTo(ReadyToExecute) <= 0 && state.internalStatus == Investigating)
                            {
                                if (fail == null && success == null)
                                {
                                    // we have globally persisted the result, so move to waiting for the result to be fully replicated amongst our shards
                                    state.executedOnAllShards(null);
                                }
                                else
                                {
                                    state.internalStatus = Waiting;
                                    if (success != null)
                                        state.recordForwardProgress(success);
                                }
                            }
                        });
                        break;
                    }
                    case ExecutedOnAllShardsButNotThisInstance:
                    {
                        Key homeKey = state.command.homeKey();
                        long homeEpoch = state.command.executeAt().epoch;
                        Shard homeShard = node.topology().forEpochIfKnown(homeKey, homeEpoch);
                        if (homeShard == null)
                        {
                            node.configService().fetchTopologyForEpoch(txnId.epoch);
                            state.internalStatus = Waiting;
                            continue;
                        }
                        checkOnCommitted(node, txnId, state.command.txn(), homeKey, homeShard, homeEpoch, txnId)
                        .whenComplete((success, fail) -> {
                            if (state.externalStatus == ExecutedOnAllShardsButNotThisInstance && state.internalStatus == Investigating)
                                state.internalStatus = NoProgress;
                        });
                        break;
                    }
                    case ExecutedAndWaitingForAllShards:
                    {
                        if (!state.command.hasBeen(Executed))
                            throw new AssertionError();

                        if (state.waitingOnExecute.isEmpty())
                        {
                            state.ensureAtLeast(ExecutedOnAllShards, Done);
                        }
                        else
                        {
                            CompletionStage<Void> sendAndCheck = applyAndCheck(node, state);
                            state.debugInProgress = sendAndCheck;
                            sendAndCheck.whenComplete((success, fail) -> {
                                if (state.waitingOnExecute.isEmpty())
                                    state.ensureAtLeast(ExecutedOnAllShards, Done);
                                else
                                    state.internalStatus = Waiting;
                            });
                        }
                        break;
                    }
                }
            }
        }
    }

    static class CoordinateApplyAndCheck extends CompletableFuture<Void> implements Callback<ApplyAndCheckOk>
    {
        final State state;
        final Set<Id> waitingOnResponses;

        static CompletionStage<Void> applyAndCheck(Node node, State state)
        {
            CoordinateApplyAndCheck coordinate = new CoordinateApplyAndCheck(state);
            Command command = state.command;
            // TODO (now): whether we need to send to future shards depends on sync logic
            Topologies topologies = node.topology().unsyncForTxn(command.txn(), command.executeAt().epoch);
            state.waitingOnExecute.retainAll(topologies.nodes()); // we might have had some nodes from older shards that are now redundant
            node.send(state.waitingOnExecute, id -> new ApplyAndCheck(id, topologies,
                                                                      command.txnId(), command.txn(), command.homeKey(),
                                                                      command.savedDeps(), command.executeAt(),
                                                                      command.writes(), command.result(),
                                                                      state.waitingOnExecute),
                      coordinate);
            return coordinate;
        }

        CoordinateApplyAndCheck(State state)
        {
            this.state = state;
            this.waitingOnResponses = new HashSet<>(state.waitingOnExecute);
        }

        @Override
        public void onSuccess(Id from, ApplyAndCheckOk response)
        {
            state.waitingOnExecute.retainAll(response.waitingOn);
            if (state.waitingOnExecute.isEmpty() || (waitingOnResponses.remove(from) && waitingOnResponses.isEmpty()))
                complete(null);
        }

        @Override
        public void onFailure(Id from, Throwable throwable)
        {
            if (waitingOnResponses.remove(from) && waitingOnResponses.isEmpty())
                complete(null);
        }
    }

    static class ApplyAndCheck extends Apply
    {
        final Set<Id> waitingOn;
        ApplyAndCheck(Id id, Topologies topologies, TxnId txnId, Txn txn, Key homeKey, Dependencies deps, Timestamp executeAt, Writes writes, Result result, Set<Id> waitingOn)
        {
            super(id, topologies, txnId, txn, homeKey, executeAt, deps, writes, result);
            this.waitingOn = waitingOn;
        }

        @Override
        public void process(Node node, Id from, ReplyContext replyContext)
        {
            Key progressKey = node.selectProgressKey(txnId, txn.keys, homeKey);
            node.reply(from, replyContext, node.mapReduceLocal(scope(), instance -> {
                Command command = instance.command(txnId);
                command.apply(txn, homeKey, progressKey, executeAt, deps, writes, result);
                if (command.handles(executeAt.epoch, progressKey))
                {
                    SimpleProgressLog.Instance log = ((SimpleProgressLog.Instance)instance.progressLog());
                    State state = log.stateMap.get(txnId);
                    state.waitingOnExecute.retainAll(waitingOn);
                    return new ApplyAndCheckOk(state.waitingOnExecute);
                }
                return null;
            }, (a, b) -> {
                if (a == null) return b;
                if (b == null) return a;
                a.waitingOn.retainAll(b.waitingOn);
                return a;
            }));
        }

        @Override
        public MessageType type()
        {
            return MessageType.APPLY_AND_CHECK_REQ;
        }

        @Override
        public String toString()
        {
            return "SendAndCheck{" +
                   "txnId:" + txnId +
                   ", txn:" + txn +
                   ", deps:" + deps +
                   ", executeAt:" + executeAt +
                   ", writes:" + writes +
                   ", result:" + result +
                   ", waitingOn:" + waitingOn +
                   '}';
        }
    }

    static class ApplyAndCheckOk implements Reply
    {
        final Set<Id> waitingOn;

        ApplyAndCheckOk(Set<Id> waitingOn)
        {
            this.waitingOn = waitingOn;
        }

        @Override
        public String toString()
        {
            return "SendAndCheckOk{" +
                "waitingOn:" + waitingOn +
                '}';
        }

        @Override
        public MessageType type()
        {
            return MessageType.APPLY_AND_CHECK_RSP;
        }
    }

    @Override
    public ProgressLog create(CommandStore commandStore)
    {
        return new Instance(commandStore);
    }
}
