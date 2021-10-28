package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.PairedValues;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.client.queries.Query5;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.KeyValueSource;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class Query5Test extends QueryTest{
    private static final String QUERY_ID = "g6q5t";
    private static final String COMMON_NAME = "T1";
    private static final String NEIGHBOURHOOD = "B1";

    private final Query5 query = new Query5();

    @Test
    @Override
    public void testQuery() throws ExecutionException, InterruptedException {
        // Create trees
        List<Tree> trees = new ArrayList<>();
        addValidTrees(trees, 23, "S1");
        addValidTrees(trees, 25, "S2");
        addValidTrees(trees, 28, "S3");
        addValidTrees(trees, 31, "S4");
        addValidTrees(trees, 15, "S5");
        addValidTrees(trees, 9, "S6");
        addValidTrees(trees, 14, "S7");
        addValidTrees(trees, 22, "S8");
        addValidTrees(trees, 39, "S9");
        addValidTrees(trees, 45, "S10");
        IList<Tree> dList = client.getList(QUERY_ID + "l");
        dList.clear();
        dList.addAll(trees);

        // Get Job
        final Job<String, Tree> job = client.getJobTracker(QUERY_ID + "j").newJob(KeyValueSource.fromList(dList));

        // Test map reduce
        final List<PairedValues> pairedValues = query.mapReduce(job, NEIGHBOURHOOD, COMMON_NAME);

        // Assertions
        Assert.assertFalse(pairedValues.isEmpty());
        PairedValues firstPair = pairedValues.get(0);
        Assert.assertNotNull(firstPair);
        Assert.assertEquals(firstPair.getCommonValue().longValue(), 30L);
        Assert.assertEquals(firstPair.getMember1(), "S4");
        Assert.assertEquals(firstPair.getMember2(), "S9");
    }

    private void addValidTrees(List<Tree> trees, int n, String street) {
        for (int i = 0; i < n; i++) {
            trees.add(new Tree(new Neighbourhood(NEIGHBOURHOOD), street, COMMON_NAME));
        }
    }
}
