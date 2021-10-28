package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.api.model.Q2NeighbourhoodTree;
import ar.edu.itba.pod.api.collators.KAscCollator;
import ar.edu.itba.pod.api.mappers.Query2CountPopulationMapper;
import ar.edu.itba.pod.api.mappers.Query2NeighbourhoodKeyMapper;
import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.reducers.Query2MaxPopReducerFactory;
import ar.edu.itba.pod.api.reducers.SumReducerFactory;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Query2 extends Query{

    private static Logger logger = LoggerFactory.getLogger(Query2.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try {
            new Query2().run();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

    }

    @Override
    public void run() throws IOException, ExecutionException, InterruptedException {
        logger.info("tpe2-g6 Query 1 Client Starting ...");


        // Parse arguments
        readArguments();

        // Config hazel
        configHazelCast();

        // Parse data
        List<Neighbourhood> neighbourhoods = getNeighbourhoods();
        List<Tree> trees = getTrees();


        String ilistName = "g6q2";
        IList<Q2NeighbourhoodTree> treeOnNeighbourhood = this.instance.getList(ilistName);
        treeOnNeighbourhood.clear();
        for (Tree t: trees) {
            if( neighbourhoods.contains(t.getNeighbourhood()) )
                treeOnNeighbourhood.add( new Q2NeighbourhoodTree(t.getNeighbourhood().getName(),t.getName(), t.getNeighbourhood().getPopulation().doubleValue()) );
        }

        KeyValueSource<String, Q2NeighbourhoodTree> source = KeyValueSource.fromList(treeOnNeighbourhood);
        JobTracker jobTracker = this.instance.getJobTracker(ilistName);


        Job<String, Q2NeighbourhoodTree> job = jobTracker.newJob(source);
        // TODO write start time
        ICompletableFuture<Map<Q2NeighbourhoodTree, Double>> future = job
                .mapper( new Query2CountPopulationMapper<>() )
                .reducer( new SumReducerFactory())
                .submit();

        Map<Q2NeighbourhoodTree, Double> result = future.get();

        String imapName = "g6q2";
        IMap<Q2NeighbourhoodTree, Double> resImap = this.instance.getMap(imapName);
        resImap.clear();
        for (Q2NeighbourhoodTree t: result.keySet()) {
           resImap.put(t, result.get(t));
        }

        KeyValueSource<Q2NeighbourhoodTree, Double> source2 = KeyValueSource.fromMap(resImap);
        Job<Q2NeighbourhoodTree, Double> job2 = jobTracker.newJob(source2);
        ICompletableFuture<List<Map.Entry<String, Q2NeighbourhoodTree>>> future2 = job2
                .mapper( new Query2NeighbourhoodKeyMapper() )
                .reducer( new Query2MaxPopReducerFactory())
                .submit(new KAscCollator<>());

        List<Map.Entry<String, Q2NeighbourhoodTree>> result2 = future2.get();
        // TODO write stop time

        //todo to output
        for (Map.Entry<String, Q2NeighbourhoodTree> e:result2) {
            System.out.println(" " + e.getKey() + " " + e.getValue().getTreeName() + " " + String.format(Locale.ROOT,"%.2f",e.getValue().getPopulation()));
        }

        this.instance.shutdown();
    }


}