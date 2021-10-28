package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.client.queries.Query3;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.KeyValueSource;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query3Test extends QueryTest{
    private static final String QUERY_ID = "g6q3t";

    private final Query3 query = new Query3();

    @Override
    public void testQuery() throws ExecutionException, InterruptedException {
        final int n = 3;

        // Create trees
        List<Tree> trees = new ArrayList<>();
        addUniqueTrees(trees, 10, 1);
        addUniqueTrees(trees, 7, 2);
        addUniqueTrees(trees, 2, 3);
        addUniqueTrees(trees, 7, 4);
        addUniqueTrees(trees, 1, 5);


        // Get list of valid Neighbourhoods
        List<String> validN = neighbourhoods.stream().map(Neighbourhood::getName).collect(Collectors.toList());

        // Get job
        IList<Tree> dList = client.getList(QUERY_ID + "l");
        dList.clear();
        dList.addAll(trees);

        final Job<String, Tree> job = client.getJobTracker(QUERY_ID + "j").newJob(KeyValueSource.fromList(dList));

        // Test map reduce
        final List<Map.Entry<String, Long>> topNNeighbourhood = query.mapReduce(validN, job, n);

        // Assertions
        Assert.assertFalse(topNNeighbourhood.isEmpty());
        Assert.assertEquals(3, topNNeighbourhood.size());

        Map.Entry<String, Long> first = topNNeighbourhood.get(0);
        Assert.assertEquals("B1", first.getKey());
        Assert.assertEquals(new Long(10), first.getValue());

        Map.Entry<String, Long> second = topNNeighbourhood.get(1);
        Assert.assertEquals("B2", second.getKey());
        Assert.assertEquals(new Long(7), second.getValue());

        Map.Entry<String, Long> third = topNNeighbourhood.get(2);
        Assert.assertEquals("B4", third.getKey());
        Assert.assertEquals(new Long(7), third.getValue());




    }

    private void addUniqueTrees(List<Tree> trees, int n, int id) {
        if (id <= 0 || id > neighbourhoods.size())
            return;
        for (int i = 0; i < n; i++) {
            trees.add(new Tree(neighbourhoods.get(id - 1), "", "T" + (i + 1)));
            trees.add(new Tree(neighbourhoods.get(id - 1), "", "T" + (i + 1)));
        }
    }
}
