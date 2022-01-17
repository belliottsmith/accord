package accord.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import accord.api.Key;
import accord.api.ProgressLog;
import accord.api.Result;
import accord.coordinate.CheckShardProgress;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.messages.Apply;
import accord.messages.Callback;
import accord.messages.CheckStatus.CheckStatusOk;
import accord.messages.CheckStatus.CheckStatusOkFull;
import accord.messages.CheckStatus.IncludeInfo;
import accord.messages.Reply;
import accord.txn.Ballot;
import accord.txn.Dependencies;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;

import static accord.coordinate.CheckHomeStatus.checkHomeStatus;
import static accord.coordinate.CheckShardProgress.checkShardProgress;
import static accord.coordinate.InformHomeOfTxn.inform;
import static accord.coordinate.MaybeRecover.maybeRecover;
import static accord.impl.SimpleProgressLog.CoordinateSendAndCheck.sendAndCheck;
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

public class SimpleProgressLog implements Runnable, Function<CommandStore, ProgressLog>
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

        Timestamp maxWaitingAt;

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

        void recordWaiter(Command waitingCommand, Command waitingOnCommand)
        {
            if (maxWaitingAt == null || maxWaitingAt.compareTo(waitingCommand.executeAt()) < 0)
                maxWaitingAt = waitingCommand.executeAt();
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
        public void waitingOn(TxnId waiting, TxnId waitingOn)
        {
            Command waitingCommand = commandStore.command(waiting);
            Command waitingOnCommand = commandStore.command(waitingOn);
            if (!waitingOnCommand.hasBeen(Status.Committed))
            {
                stateMap.computeIfAbsent(waitingOn, State::new)
                        .ensureAtLeast(NonHomeWaitingOn, Waiting, waitingOnCommand)
                        .recordWaiter(waitingCommand, waitingOnCommand);
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
            Set<Id> nodes = new HashSet<>(node.cluster().forKeys(command.txn().keys).nodes());
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
                    if (state.command.homeKey() == null)
                    {
                        // check status with the only keys we know, if any, then:
                        // 1. if we cannot find any primary record of the transaction, then it cannot be a dependency so record this fact
                        // 2. otherwise record the homeKey for future reference and set the status based on whether progress has been made
                        Timestamp maxWaitingAt = state.maxWaitingAt;
                        Key key = state.command.someKey();
                        CheckShardProgress check = checkShardProgress(node, txnId, state.command.txn(), key, node.cluster().forKey(key),
                                                                      state.maxStatus, state.maxPromised, state.maxPromiseHasBeenAccepted,
                                                                      IncludeInfo.HomeKey.and(IncludeInfo.Dependencies.and(IncludeInfo.ExecuteAt)));
                        state.inProgress = check;
                        check.whenComplete((success, fail) -> {
                            if (state.externalStatus != NonHomeWaitingOn)
                                return;

                            if (fail != null)
                            {
                                state.internalStatus = NoProgress;
                            }
                            else if (success.status.compareTo(PreAccepted) < 0)
                            {
                                // cannot be a dependency
                                state.command.mustExecuteAfter(maxWaitingAt);
                                if (state.maxWaitingAt.equals(maxWaitingAt))
                                {
                                    state.externalStatus = NonHomeSafe;
                                    state.internalStatus = NoProgressExpected;
                                }
                                else
                                {
                                    state.internalStatus = Waiting;
                                }
                            }
                            else
                            {
                                CheckStatusOkFull full = (CheckStatusOkFull) success;
                                // TODO: record this against all local command stores containing the transaction
                                state.command.homeKey(full.homeKey);
                                if (success.status.compareTo(Status.Committed) >= 0)
                                {
                                    state.externalStatus = NonHomeSafe;
                                    state.internalStatus = NoProgressExpected;
                                    state.command.commit(null, full.homeKey, full.deps, full.executeAt);
                                }
                                else if (check.hasMadeProgress())
                                {
                                    state.recordCheckStatus(success);
                                    state.internalStatus = Waiting;
                                }
                                else
                                {
                                    state.internalStatus = NoProgress;
                                }
                            }
                        });
                    }
                    else
                    {
                        Timestamp maxWaitingAt = state.maxWaitingAt;
                        CompletionStage<CheckStatusOk> check = checkHomeStatus(node, txnId, state.command.txn(), state.command.homeKey(),
                                                                               node.cluster().forKey(state.command.homeKey()),
                                                                               IncludeInfo.HomeKey.and(IncludeInfo.Dependencies.and(IncludeInfo.ExecuteAt)));
                        state.inProgress = check;
                        check.whenComplete((success, fail) -> {
                            if (state.externalStatus != NonHomeWaitingOn)
                                return; // TODO: merge with above

                            try
                            {
                                if (fail != null) state.internalStatus = NoProgress;
                                else if (success.status.compareTo(Status.Committed) >= 0)
                                {
                                    CheckStatusOkFull ok = (CheckStatusOkFull) success;
                                    state.command.commit(null, ok.homeKey, ok.deps, ok.executeAt);
                                    state.externalStatus = NonHomeSafe;
                                    state.internalStatus = NoProgressExpected;
                                }
                                else
                                {
                                    state.command.mustExecuteAfter(maxWaitingAt);
                                    if (state.maxWaitingAt.equals(maxWaitingAt))
                                    {
                                        state.externalStatus = NonHomeSafe;
                                        state.internalStatus = NoProgressExpected;
                                    }
                                    else
                                    {
                                        state.internalStatus = Waiting;
                                    }
                                }
                            }
                            catch (Throwable t)
                            {
                                state.internalStatus = NoProgress;
                                throw t;
                            }
                        });
                    }
                    break;
                }

                case Uncommitted:
                case ReadyToExecute:
                {
                    CompletionStage<CheckStatusOk> recover = maybeRecover(node, txnId, state.command.txn(),
                                                                          state.command.homeKey(), node.cluster().forKey(state.command.homeKey()),
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
                        CompletionStage<Void> sendAndCheck = sendAndCheck(node, state);
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

    static class CoordinateSendAndCheck extends CompletableFuture<Void> implements Callback<SendAndCheckOk>
    {
        final State state;
        final Set<Id> waitingOnResponses;

        static CompletionStage<Void> sendAndCheck(Node node, State state)
        {
            CoordinateSendAndCheck coordinate = new CoordinateSendAndCheck(state);
            Command command = state.command;
            node.send(state.waitingOn, new SendAndCheck(command.txnId(), command.txn(), command.homeKey(),
                                                        command.savedDeps(), command.executeAt(),
                                                        command.writes(), command.result(),
                                                        state.waitingOn),
                      coordinate);
            return coordinate;
        }

        CoordinateSendAndCheck(State state)
        {
            this.state = state;
            this.waitingOnResponses = new HashSet<>(state.waitingOn);
        }

        @Override
        public void onSuccess(Id from, SendAndCheckOk response)
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

    static class SendAndCheck extends Apply
    {
        final Set<Id> waitingOn;
        SendAndCheck(TxnId txnId, Txn txn, Key homeKey, Dependencies deps, Timestamp executeAt, Writes writes, Result result, Set<Id> waitingOn)
        {
            super(txnId, txn, homeKey, executeAt, deps, writes, result);
            this.waitingOn = waitingOn;
        }

        @Override
        public void process(Node on, Id from, long messageId)
        {
            on.reply(from, messageId, txn.local(on).map(instance -> {
                instance.command(txnId).apply(txn, homeKey, deps, executeAt, writes, result);
                SimpleProgressLog.Instance log = ((SimpleProgressLog.Instance)instance.progressLog());
                State state = log.parent().stateMap.get(txnId);
                state.waitingOn.retainAll(waitingOn);
                return new SendAndCheckOk(state.waitingOn);
            }).reduce((a, b) -> {
                a.waitingOn.retainAll(b.waitingOn);
                return a;
            }).orElseThrow());
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

    static class SendAndCheckOk implements Reply
    {
        final Set<Id> waitingOn;

        SendAndCheckOk(Set<Id> waitingOn)
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
    }

    @Override
    public ProgressLog apply(CommandStore commandStore)
    {
        return new Instance(commandStore);
    }
}
