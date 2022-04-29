package accord.impl;

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

import static accord.coordinate.CheckOnUncommitted.checkOnUncommitted;
import static accord.coordinate.InformHomeOfTxn.inform;
import static accord.coordinate.MaybeRecover.maybeRecover;
import static accord.impl.SimpleProgressLog.CoordinateApplyAndCheck.applyAndCheck;
import static accord.impl.SimpleProgressLog.ExternalStatus.Committed;
import static accord.impl.SimpleProgressLog.ExternalStatus.NonHomePreAccept;
import static accord.impl.SimpleProgressLog.ExternalStatus.NonHomeSafe;
import static accord.impl.SimpleProgressLog.ExternalStatus.NonHomeWaitingOn;
import static accord.impl.SimpleProgressLog.ExternalStatus.ReadyToExecute;
import static accord.impl.SimpleProgressLog.ExternalStatus.ExecutedOnAllShards;
import static accord.impl.SimpleProgressLog.ExternalStatus.Uncommitted;
import static accord.impl.SimpleProgressLog.ExternalStatus.WaitingForExecutedOnAllShards;
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
        ReadyToExecute, Executed,
        WaitingForExecutedOnAllShards, ExecutedOnAllShards
    }
    enum InternalStatus { Waiting, NoProgress, Investigating, NoProgressExpected, Done }

    static class State
    {
        ExternalStatus externalStatus;
        InternalStatus internalStatus = Waiting;

        Set<Id> waitingOn; // TODO: limit to the shards owned by the node
        Command command;

        Status maxStatus;
        Ballot maxPromised;
        boolean maxPromiseHasBeenAccepted;

        Timestamp maxExecuteAtWithTxnAsDependency;

        Object inProgress;

        State(TxnId ignore) {}

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

        void setApplied(Set<Id> waitingOn, Command command)
        {
            if (this.waitingOn == null)
            {
                this.waitingOn = waitingOn;
                setCommand(command);
            }
        }

        void setCommand(Command command)
        {
            this.command = command;
            if (maxStatus == null || maxStatus.compareTo(command.status()) < 0)
                maxStatus = command.status();
            if (maxPromised == null || maxPromised.compareTo(command.promised()) < 0)
                maxPromised = command.promised();
            maxPromiseHasBeenAccepted |= command.accepted().equals(maxPromised);
        }

        void recordBlocker(Command waitingCommand, Command blockedByUncommittedCommand)
        {
            Preconditions.checkArgument(blockedByUncommittedCommand.txnId().equals(command.txnId()));
            if (maxExecuteAtWithTxnAsDependency == null || maxExecuteAtWithTxnAsDependency.compareTo(waitingCommand.executeAt()) < 0)
            {
                maxExecuteAtWithTxnAsDependency = waitingCommand.executeAt();
            }
        }

        void recordCheckStatus(CheckStatusOk ok)
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

        @Override
        public void nonHomePreaccept(TxnId txnId)
        {
            stateMap.computeIfAbsent(txnId, State::new)
                    .ensureAtLeast(NonHomePreAccept, Waiting, commandStore.command(txnId));
        }

        @Override
        public void nonHomePostPreaccept(TxnId txnId)
        {
            stateMap.computeIfAbsent(txnId, State::new)
                    .ensureAtLeast(NonHomeSafe, NoProgressExpected, commandStore.command(txnId));
        }

        @Override
        public void nonHomeCommit(TxnId txnId)
        {
            State state = stateMap.get(txnId);
            if (state.externalStatus == NonHomeWaitingOn)
            {
                state.externalStatus = NonHomeSafe;
                state.internalStatus = NoProgressExpected;
            }
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

        @Override
        public void uncommitted(TxnId txnId)
        {
            stateMap.computeIfAbsent(txnId, State::new)
                    .ensureAtLeast(Uncommitted, Waiting, commandStore.command(txnId));
        }

        @Override
        public void committed(TxnId txnId)
        {
            stateMap.computeIfAbsent(txnId, State::new)
                    .ensureAtLeast(Committed, NoProgressExpected, commandStore.command(txnId));
        }

        @Override
        public void readyToExecute(TxnId txnId)
        {
            stateMap.computeIfAbsent(txnId, State::new)
                    .ensureAtLeast(ReadyToExecute, Waiting, commandStore.command(txnId));
        }

        @Override
        public void executedOnAllShards(TxnId txnId)
        {
            stateMap.computeIfAbsent(txnId, State::new)
                    .ensureAtLeast(ExternalStatus.Executed, NoProgressExpected, commandStore.command(txnId));
        }

        @Override
        public void executed(TxnId txnId)
        {
            Command command = commandStore.command(txnId);
            // TODO (now): this should be limited to the replicas of this shard?
            Set<Id> nodes = new HashSet<>(node.topology().forTxn(command.txn(), command.executeAt().epoch).nodes());
            nodes.remove(commandStore.node().id());

            stateMap.computeIfAbsent(txnId, State::new)
                    .ensureAtLeast(WaitingForExecutedOnAllShards, Waiting, commandStore.command(txnId))
                    .setApplied(nodes, command);
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
                    state.inProgress = inform;
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
                    Timestamp maxExecuteAtWithTxnAsDependency = state.maxExecuteAtWithTxnAsDependency;
                    Key key = state.command.someKey();
                    Shard someShard = node.topology().forEpochIfKnown(key, txnId.epoch);
                    if (someShard == null)
                    {
                        node.configService().fetchTopologyForEpoch(txnId.epoch);
                        state.internalStatus = Waiting;
                        continue;
                    }
                    CheckOnUncommitted check = checkOnUncommitted(node, txnId, state.command.txn(),
                                                                  key, someShard,
                                                                  state.maxExecuteAtWithTxnAsDependency);
                    state.inProgress = check;
                    check.whenComplete((success, fail) -> {
                        if (state.externalStatus != NonHomeWaitingOn)
                            return;

                        // set the new status immediately to account for exceptions;
                        // we retry at the next interval in all cases as this operation is non-competitive
                        state.internalStatus = NoProgress;
                        if (fail != null) // TODO: log?
                            return;

                        state.recordCheckStatus(success);
                        if ((success.status.compareTo(PreAccepted) < 0 && state.maxExecuteAtWithTxnAsDependency.equals(maxExecuteAtWithTxnAsDependency))
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
                    long homeEpoch = (state.externalStatus == Uncommitted ? txnId : state.command.executeAt()).epoch;
                    Shard homeShard = node.topology().forEpochIfKnown(homeKey, homeEpoch);
                    if (homeShard == null)
                    {
                        node.configService().fetchTopologyForEpoch(txnId.epoch);
                        state.internalStatus = Waiting;
                        continue;
                    }
                    CompletionStage<CheckStatusOk> recover = maybeRecover(node, txnId, state.command.txn(),
                                                                          homeKey, homeShard,
                                                                          state.maxStatus, state.maxPromised, state.maxPromiseHasBeenAccepted);
                    state.inProgress = recover;
                    recover.whenComplete((success, fail) -> {
                        if (state.externalStatus.compareTo(ReadyToExecute) <= 0 && state.internalStatus == Investigating)
                        {
                            if (fail == null && success == null)
                            {
                                // we have globally persisted the result, so move to waiting for the result to be fully replicated amongst our shards
                                stateMap.computeIfAbsent(txnId, State::new)
                                        .ensureAtLeast(ExternalStatus.Executed, NoProgressExpected, null);
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

                case WaitingForExecutedOnAllShards:
                {

                    if (!state.command.hasBeen(Executed))
                        throw new AssertionError();

                    if (state.waitingOn.isEmpty())
                    {
                        state.ensureAtLeast(ExecutedOnAllShards, Done);
                    }
                    else
                    {
                        CompletionStage<Void> sendAndCheck = applyAndCheck(node, state);
                        state.inProgress = sendAndCheck;
                        sendAndCheck.whenComplete((success, fail) -> {
                            if (state.waitingOn.isEmpty())
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
            Topologies topologies = node.topology().forTxn(command.txn(), command.executeAt().epoch);
            state.waitingOn.retainAll(topologies.nodes()); // we might have had some nodes from older shards that are now redundant
            node.send(state.waitingOn, id -> new ApplyAndCheck(id, topologies,
                                                         command.txnId(), command.txn(), command.homeKey(),
                                                         command.savedDeps(), command.executeAt(),
                                                         command.writes(), command.result(),
                                                         state.waitingOn),
                      coordinate);
            return coordinate;
        }

        CoordinateApplyAndCheck(State state)
        {
            this.state = state;
            this.waitingOnResponses = new HashSet<>(state.waitingOn);
        }

        @Override
        public void onSuccess(Id from, ApplyAndCheckOk response)
        {
            state.waitingOn.retainAll(response.waitingOn);
            if (state.waitingOn.isEmpty() || (waitingOnResponses.remove(from) && waitingOnResponses.isEmpty()))
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
                state.waitingOn.retainAll(waitingOn);
                return new ApplyAndCheckOk(state.waitingOn);
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
