package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.PairedValues;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.client.queries.Query4;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.KeyValueSource;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query4Test extends QueryTest {
    private static final String QUERY_ID = "g6q4t";

    private final Query4 query = new Query4();

    @Test
    @Override
    public void testQuery() throws ExecutionException, InterruptedException {
        // Create trees
        List<Tree> trees = new ArrayList<>();
        addUniqueTrees(trees, 325, 1);
        addUniqueTrees(trees, 312, 2);
        addUniqueTrees(trees, 1512, 3);
        addUniqueTrees(trees, 1500, 4);
        addUniqueTrees(trees, 150, 5);
        addUniqueTrees(trees, 101, 6);
        addUniqueTrees(trees, 90, 7);
        addUniqueTrees(trees, 80, 8);
        addUniqueTrees(trees, 356, 9);
        addUniqueTrees(trees, 380, 10);

        // Get list of valid Neighbourhoods
        String invalidN = neighbourhoods.get(10 - 1).getName();
        List<String> validN = neighbourhoods.stream().map(Neighbourhood::getName).filter(name -> !name.equals(invalidN)).collect(Collectors.toList());

        // Get job
        IList<Tree> dList = client.getList(QUERY_ID + "l");
        dList.clear();
        dList.addAll(trees);

        final Job<String, Tree> job = client.getJobTracker(QUERY_ID + "j").newJob(KeyValueSource.fromList(dList));

        // Test map reduce
        final List<PairedValues> pairedValues = query.mapReduce(job, validN);

        // Assertions
        Assert.assertFalse(pairedValues.isEmpty());
        PairedValues firstPair = pairedValues.get(0);
        Assert.assertEquals(firstPair.getCommonValue().longValue(), 1500L);
        Assert.assertEquals(firstPair.getMember1(), neighbourhoods.get(3 - 1).getName());
        Assert.assertEquals(firstPair.getMember2(), neighbourhoods.get(4 - 1).getName());
        Assert.assertFalse(pairedValues.stream().anyMatch(pv -> pv.getMember1().equals(invalidN) || pv.getMember2().equals(invalidN)));
        Assert.assertFalse(pairedValues.stream().anyMatch(pv -> pv.getCommonValue() < 100L));
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
