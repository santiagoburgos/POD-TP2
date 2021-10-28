package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.PairCompoundKeyValue;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.client.queries.Query2;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.KeyValueSource;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query2Test extends QueryTest {
    private static final String QUERY_ID = "g6q2";

    private final Query2 query = new Query2();

    @Override
    public void testQuery() throws ExecutionException, InterruptedException {
        // Create trees
        // Create trees
        List<Tree> trees = new ArrayList<>();
        addTrees(trees, 2000, 1); //T0
        addTrees(trees, 200, 1);
        addTrees(trees, 40, 3);
        addTrees(trees, 62000, 3); //T3
        addTrees(trees, 62000, 3); //T4
        addTrees(trees, 62000, 3); //T5
        addTrees(trees, 5, 5);
        addTrees(trees, 600, 1);
        addTrees(trees, 31000, 3); //T8
        addTrees(trees, 24000, 3);

        // Get list of valid Neighbourhoods
        String invalidN = neighbourhoods.get(10 - 1).getName();
        List<String> validN = neighbourhoods.stream().map(Neighbourhood::getName).filter(name -> !name.equals(invalidN)).collect(Collectors.toList());

        // Get job
        IList<PairCompoundKeyValue> dList = client.getList(QUERY_ID + "1");
        dList.clear();
        for (Tree t: trees) {
            dList.add(new PairCompoundKeyValue(t.getNeighbourhood().getName(),t.getName(), t.getNeighbourhood().getPopulation().doubleValue()) );
        }

        final Job<String, PairCompoundKeyValue> job = client.getJobTracker(QUERY_ID).newJob(KeyValueSource.fromList(dList));

        final Map<PairCompoundKeyValue, Double> result1 = query.mapReduce1(job, validN);

        IMap<PairCompoundKeyValue, Double> resImap = client.getMap(QUERY_ID + "2");
        resImap.clear();
        for (PairCompoundKeyValue t: result1.keySet()) {
            resImap.put(t, result1.get(t));
        }

        final Job<PairCompoundKeyValue, Double>  job2 = client.getJobTracker(QUERY_ID).newJob(KeyValueSource.fromMap(resImap));

        List<Map.Entry<String, PairCompoundKeyValue>> result2 = query.mapReduce2(job2);

        Assert.assertFalse(result2.isEmpty());
        Map.Entry<String, PairCompoundKeyValue> first = result2.get(0);
        Map.Entry<String, PairCompoundKeyValue> second = result2.get(1);

        Map.Entry<String, PairCompoundKeyValue> third = result2.get(2);

        Assert.assertEquals(first.getValue().getK1(), "B1");
        Assert.assertEquals(first.getValue().getK2(), "T0");
        Assert.assertEquals(first.getValue().getValue().doubleValue(), 1d, 1E-2);
        Assert.assertEquals(second.getValue().getK1(), "B3");
        Assert.assertEquals(second.getValue().getK2(), "T3");
        Assert.assertEquals(second.getValue().getValue().doubleValue(), 2d, 1E-2);

    }

    private int tid = 0;
    private void addTrees(List<Tree> trees, int n, int id) {
        if (id <= 0 || id > neighbourhoods.size())
            return;
        for (int i = 0; i < n; i++) {
            trees.add(new Tree(neighbourhoods.get(id - 1), "", "T" + tid));
        }
        tid++;
    }
}