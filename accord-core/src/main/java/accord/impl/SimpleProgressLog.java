package accord.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.coordinate.CheckOnUncommitted;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.Apply;
import accord.messages.Callback;
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

import static accord.coordinate.CheckOnExecuted.checkOnExecuted;
import static accord.coordinate.CheckOnUncommitted.checkOnUncommitted;
import static accord.coordinate.InformHomeOfTxn.inform;
import static accord.coordinate.MaybeRecover.maybeRecover;
import static accord.impl.SimpleProgressLog.CoordinateApplyAndCheck.applyAndCheck;
import static accord.impl.SimpleProgressLog.ExternalStatus.Committed;
import static accord.impl.SimpleProgressLog.ExternalStatus.CommittedWontExecute;
import static accord.impl.SimpleProgressLog.ExternalStatus.ExecutedOnAllShardsButNotThisInstance;
import static accord.impl.SimpleProgressLog.ExternalStatus.NonHomePreAccept;
import static accord.impl.SimpleProgressLog.ExternalStatus.NonHomeSafe;
import static accord.impl.SimpleProgressLog.ExternalStatus.NonHomeWaitingOn;
import static accord.impl.SimpleProgressLog.ExternalStatus.ReadyToExecute;
import static accord.impl.SimpleProgressLog.ExternalStatus.ExecutedOnAllShards;
import static accord.impl.SimpleProgressLog.ExternalStatus.Uncommitted;
import static accord.impl.SimpleProgressLog.ExternalStatus.ExecutedAndWaitingForAllShards;
import static accord.impl.SimpleProgressLog.InternalStatus.Done;
import static accord.impl.SimpleProgressLog.InternalStatus.Investigating;
import static accord.impl.SimpleProgressLog.InternalStatus.NoProgress;
import static accord.impl.SimpleProgressLog.InternalStatus.NoProgressExpected;
import static accord.impl.SimpleProgressLog.InternalStatus.Waiting;
import static accord.local.Status.Executed;
import static accord.local.Status.PreAccepted;

public class SimpleProgressLog implements Runnable, ProgressLog.Factory
{
    enum ExternalStatus
    {
        NonHomePreAccept, NonHomeSafe, NonHomeWaitingOn,
        Uncommitted, Committed,
        ReadyToExecute, CommittedWontExecute,
        ExecutedAndWaitingForAllShards, ExecutedOnAllShardsButNotThisInstance, ExecutedOnAllShards
    }
    enum InternalStatus { Waiting, NoProgress, Investigating, NoProgressExpected, Done }

    static class State
    {
        final TxnId txnId;

        Txn txn;
        Key someKey, homeKey;

        ExternalStatus externalStatus;
        InternalStatus internalStatus = Waiting;
        Set<Id> waitingOnExecute; // TODO: limit to the shards owned by the node

        Status status;
        Ballot promised;
        boolean promiseHasBeenAccepted;

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
            if (status == null || status.compareTo(command.status()) < 0)
                status = command.status();
            if (promised == null || promised.compareTo(command.promised()) < 0)
                promised = command.promised();
            promiseHasBeenAccepted |= command.accepted().equals(promised);
        }

        void recordBlocker(Command waitingCommand, Command blockedByUncommittedCommand)
        {
            Preconditions.checkArgument(blockedByUncommittedCommand.txnId().equals(txnId));
            if (blockingTxnWithExecuteAt == null || blockingTxnWithExecuteAt.compareTo(waitingCommand.executeAt()) < 0)
            {
                blockingTxnWithExecuteAt = waitingCommand.executeAt();
            }
        }

        void recordCheckStatus(CheckStatusOk ok)
        {
            if (ok.status.compareTo(status) > 0) status = ok.status;
            if (ok.promised.compareTo(promised) > 0)
            {
                promised = ok.promised;
                promiseHasBeenAccepted = ok.accepted.equals(ok.promised);
            }
            else if (ok.promised.equals(promised))
            {
                promiseHasBeenAccepted |= ok.accepted.equals(ok.promised);
            }
        }

        void executedOnAllShards(Command newCommand)
        {
            if (newCommand != null)
                setCommand(newCommand);

            if (waitingOnExecute == null)
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

        void executed(Node node, Command command)
        {
            setCommand(command);

            // TODO (now): this should be limited to the replicas of this shard?

            switch (externalStatus)
            {
                default: throw new IllegalStateException();
                case NonHomeWaitingOn:
                case NonHomePreAccept:
                case NonHomeSafe:
                case Uncommitted:
                case Committed:
                case ReadyToExecute:
                    waitingOnExecute = new HashSet<>(node.topology().unsyncForTxn(this.command.txn(), this.command.executeAt().epoch).nodes());
                    waitingOnExecute.remove(node.id());
                    externalStatus = ExecutedAndWaitingForAllShards;
                    break;
                case ExecutedOnAllShardsButNotThisInstance:
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
    final Map<TxnId, State> stateMap = new HashMap<>();

    public SimpleProgressLog(Node node)
    {
        this.node = node;
        node.scheduler().recurring(this, 200L, TimeUnit.MILLISECONDS);
    }

    class Instance implements ProgressLog
    {
        final CommandStore commandStore;

        Instance(CommandStore commandStore)
        {
            this.commandStore = commandStore;
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
            if (state != null && state.externalStatus == NonHomeWaitingOn)
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
        public void commit(TxnId txnId, boolean isHomeCommitShard, boolean isHomeExecuteShard)
        {
            if (!isHomeCommitShard && !isHomeExecuteShard)
            {
                nonHomeCommit(txnId);
            }
            else
            {
                Command command = commandStore.command(txnId);
                stateMap.computeIfAbsent(txnId, State::new)
                        .ensureAtLeast(isHomeExecuteShard ? Committed : CommittedWontExecute, NoProgressExpected, command);
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
                    .executed(node, command);
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
            if (waitingCommand.hasBeen(Status.Committed) && !blockedByCommand.hasBeen(Status.Committed))
            {
                stateMap.computeIfAbsent(blockedBy, State::new)
                        .ensureAtLeast(NonHomeWaitingOn, Waiting, blockedByCommand)
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
        for (Map.Entry<TxnId, State> entry : stateMap.entrySet())
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

            state.internalStatus = Investigating;
            switch (state.externalStatus)
            {
                default:
                case ExecutedOnAllShards:
                    throw new AssertionError();

                case NonHomePreAccept:
                {
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

                case NonHomeWaitingOn:
                {
                    // check status with the only keys we know, if any, then:
                    // 1. if we cannot find any primary record of the transaction, then it cannot be a dependency so record this fact
                    // 2. otherwise record the homeKey for future reference and set the status based on whether progress has been made
                    Timestamp maxExecuteAtWithTxnAsDependency = state.blockingTxnWithExecuteAt;
                    Key key = state.command.someKey();
                    long epoch = state.command.is(Status.Committed) ? state.command.executeAt().epoch : txnId.epoch;
                    Shard someShard = node.topology().forEpochIfKnown(key, epoch);
                    if (someShard == null)
                    {
                        node.configService().fetchTopologyForEpoch(txnId.epoch);
                        state.internalStatus = Waiting;
                        continue;
                    }
                    // TODO (now): if we have a quorum of PreAccept and we have contacted the home shard then we can set NonHomeSafe
                    CheckOnUncommitted check = checkOnUncommitted(node, txnId, state.command.txn(),
                                                                  key, someShard, epoch,
                                                                  state.blockingTxnWithExecuteAt);
                    state.debugInProgress = check;
                    check.whenComplete((success, fail) -> {
                        if (state.externalStatus != NonHomeWaitingOn)
                            return;

                        // set the new status immediately to account for exceptions;
                        // we retry at the next interval in all cases as this operation is non-competitive
                        state.internalStatus = NoProgress;
                        if (fail != null) // TODO: log?
                            return;

                        state.recordCheckStatus(success);
                        if ((success.status.compareTo(PreAccepted) < 0 && state.blockingTxnWithExecuteAt.equals(maxExecuteAtWithTxnAsDependency))
                            || success.status.compareTo(Status.Committed) >= 0)
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
                                                                          state.status, state.promised, state.promiseHasBeenAccepted);
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
                                    state.recordCheckStatus(success);
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
                    checkOnExecuted(node, txnId, state.command.txn(),homeKey, homeShard, homeEpoch)
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

    static class CoordinateApplyAndCheck extends CompletableFuture<Void> implements Callback<ApplyAndCheckOk>
    {
        final State state;
        final Set<Id> waitingOnResponses;

        static CompletionStage<Void> applyAndCheck(Node node, State state)
        {
            CoordinateApplyAndCheck coordinate = new CoordinateApplyAndCheck(state);
            Command command = state.command;
            // TODO (now): whether we need to send to future shards depends on sync logic
            Topologies topologies = node.topology().unsyncForTxn(command.txn(), command.executeAt().epoch, Long.MAX_VALUE);
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
        public void process(Node on, Id from, ReplyContext replyContext)
        {
            on.reply(from, replyContext, on.mapReduceLocal(scope(), instance -> {
                instance.command(txnId).apply(txn, homeKey, executeAt, deps, writes, result);
                SimpleProgressLog.Instance log = ((SimpleProgressLog.Instance)instance.progressLog());
                State state = log.parent().stateMap.get(txnId);
                state.waitingOnExecute.retainAll(waitingOn);
                return new ApplyAndCheckOk(state.waitingOnExecute);
            }, (a, b) -> {
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
