package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.PairCompoundKeyValue;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.client.queries.Query1;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.KeyValueSource;
import junit.framework.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query1Test extends QueryTest {
    private static final String QUERY_ID = "g6q1";

    private final Query1 query = new Query1();

    @Override
    public void testQuery() throws ExecutionException, InterruptedException {
        // Create trees
        // Create trees
        List<Tree> trees = new ArrayList<>();
        addTrees(trees, 2000, 1);
        addTrees(trees, 312, 2);
        addTrees(trees, 1512, 3);
        addTrees(trees, 1500, 4);
        addTrees(trees, 150, 5);
        addTrees(trees, 101, 6);
        addTrees(trees, 90, 7);
        addTrees(trees, 80, 8);
        addTrees(trees, 356, 9);
        addTrees(trees, 380, 10);



        // Get list of valid Neighbourhoods
        String invalidN = neighbourhoods.get(10 - 1).getName();
        List<String> validN = neighbourhoods.stream().map(Neighbourhood::getName).filter(name -> !name.equals(invalidN)).collect(Collectors.toList());

        // Get job
        IList< String> dList = client.getList(QUERY_ID);
        dList.clear();
        for (Tree t: trees) {
            dList.add( t.getNeighbourhood().getName());
        }

        final Job<String, String> job = client.getJobTracker(QUERY_ID).newJob(KeyValueSource.fromList(dList));

        final List<Map.Entry<String, Double>> result = query.mapReduce(job, validN);

        Assert.assertFalse(result.isEmpty());
        Map.Entry<String, Double> first = result.get(0);
        Map.Entry<String, Double> second = result.get(1);

        Assert.assertEquals(first.getKey(), "B1");
        Assert.assertEquals(first.getValue().longValue(), 2000l);
        Assert.assertEquals(second.getKey(), "B3");
        Assert.assertEquals(second.getValue().longValue(), 1512l);

    }

    private void addTrees(List<Tree> trees, int n, int id) {
        if (id <= 0 || id > neighbourhoods.size())
            return;
        for (int i = 0; i < n; i++) {
            trees.add(new Tree(neighbourhoods.get(id - 1), "", "T" + (i + 1)));
        }
    }
}
