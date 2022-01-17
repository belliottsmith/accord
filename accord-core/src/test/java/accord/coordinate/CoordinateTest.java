package accord.coordinate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import accord.local.Node;
import accord.impl.mock.MockCluster;
import accord.api.Result;
import accord.impl.mock.MockStore;
import accord.txn.Keys;
import accord.txn.Txn;
import accord.txn.TxnId;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static accord.Utils.id;
import static accord.Utils.ids;
import static accord.Utils.writeTxn;
import static accord.impl.IntKey.keys;

public class CoordinateTest
{
    @Test
    void simpleTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().build())
        {
            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            TxnId txnId = new TxnId(1, 100, 0, node.id());
            Txn txn = writeTxn(keys(10));
<<<<<<< HEAD
            Result result = Coordinate.execute(node, txnId, txn).get();
=======
            CompletionStage<Result> stage = Coordinate.execute(node, txnId, txn, txn.keys().get(0));
            Result result = stage.toCompletableFuture().get();
>>>>>>> 9e2cbf0 (first draft)
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }

    @Test
    void slowPathTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(7).replication(7).build())
        {
            cluster.networkFilter.isolate(ids(5, 7));

            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

<<<<<<< HEAD
            TxnId txnId = new TxnId(1, 100, 0, node.id());
            Txn txn = writeTxn(keys(10));
            Result result = Coordinate.execute(node, txnId, txn).get();
=======
            Txn txn = writeTxn(keys(10));
            Result result = cluster.get(id(1)).coordinate(txn).toCompletableFuture().get();
>>>>>>> 9e2cbf0 (first draft)
            Assertions.assertEquals(MockStore.RESULT, result);
        }
    }

    private TxnId coordinate(Node node, long clock, Keys keys) throws Throwable
    {
        TxnId txnId = new TxnId(1, clock, 0, node.id());
        Txn txn = writeTxn(keys);
<<<<<<< HEAD
        Result result = Coordinate.execute(node, txnId, txn).get();
=======
        Result result = Coordinate.execute(node, txnId, txn, node.selectHomeKey(txn.keys))
                                  .toCompletableFuture().get();
>>>>>>> 9e2cbf0 (first draft)
        Assertions.assertEquals(MockStore.RESULT, result);
        return txnId;
    }

    @Test
    void multiKeyTest() throws Throwable
    {
        try (MockCluster cluster = MockCluster.builder().nodes(6).maxKey(600).build())
        {
            Node node = cluster.get(1);
            Assertions.assertNotNull(node);

            TxnId txnId1 = coordinate(node, 100, keys(50, 350, 550));
            TxnId txnId2 = coordinate(node, 150, keys(250, 350, 450));
            TxnId txnId3 = coordinate(node, 125, keys(50, 60, 70, 80, 350, 550));
        }
    }
}
