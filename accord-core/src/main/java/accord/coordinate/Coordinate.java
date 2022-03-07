package accord.coordinate;

import accord.api.ConfigurationService;
import accord.api.Result;
import accord.local.Node;
import accord.txn.Ballot;
import accord.txn.Txn;
import accord.txn.TxnId;

import java.util.concurrent.CompletionStage;

public class Coordinate
{
    private static CompletionStage<Result> fetchEpochOrExecute(Node node, Agreed agreed)
    {
        long executeEpoch = agreed.executeAt.epoch;
        ConfigurationService configService = node.configService();
        if (executeEpoch > configService.currentEpoch())
            return configService.fetchTopologyForEpochStage(executeEpoch)
                                .thenCompose(v -> fetchEpochOrExecute(node, agreed));

        CompletionStage<Result> execute = Execute.execute(node, agreed);
        return execute;
    }

    private static CompletionStage<Result> andThenExecute(Node node, CompletionStage<Agreed> agree)
    {
        return agree.thenCompose(agreed -> fetchEpochOrExecute(node, agreed));
    }

    public static CompletionStage<Result> execute(Node node, TxnId txnId, Txn txn)
    {
        return andThenExecute(node, Agree.agree(node, txnId, txn));
    }

    public static CompletionStage<Result> recover(Node node, TxnId txnId, Txn txn)
    {
        return andThenExecute(node, new Recover(node, new Ballot(node.uniqueNow()), txnId, txn));
    }
}
