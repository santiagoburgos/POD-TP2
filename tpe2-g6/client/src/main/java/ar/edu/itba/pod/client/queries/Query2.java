package ar.edu.itba.pod.client.queries;


import ar.edu.itba.pod.api.collators.KAscCollator;

import ar.edu.itba.pod.api.mappers.CountOverValueMapper;
import ar.edu.itba.pod.api.mappers.Key1PairMapper;
import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.PairCompoundKeyValue;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.reducers.MaxValueReducerFactory;
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
        IList<PairCompoundKeyValue> treeOnNeighbourhood = this.instance.getList(ilistName);
        treeOnNeighbourhood.clear();
        for (Tree t: trees) {
            if( neighbourhoods.contains(t.getNeighbourhood()) )
                treeOnNeighbourhood.add( new PairCompoundKeyValue(t.getNeighbourhood().getName(),t.getName(), t.getNeighbourhood().getPopulation().doubleValue()) );
        }

        KeyValueSource<String, PairCompoundKeyValue> source = KeyValueSource.fromList(treeOnNeighbourhood);
        JobTracker jobTracker = this.instance.getJobTracker(ilistName);


        Job<String, PairCompoundKeyValue> job = jobTracker.newJob(source);
        // TODO write start time
        ICompletableFuture<Map<PairCompoundKeyValue, Double>> future = job
                .mapper( new CountOverValueMapper<>() )
                .reducer( new SumReducerFactory())
                .submit();

        Map<PairCompoundKeyValue, Double> result = future.get();

        String imapName = "g6q2";
        IMap<PairCompoundKeyValue, Double> resImap = this.instance.getMap(imapName);
        resImap.clear();
        for (PairCompoundKeyValue t: result.keySet()) {
           resImap.put(t, result.get(t));
        }

        KeyValueSource<PairCompoundKeyValue, Double> source2 = KeyValueSource.fromMap(resImap);
        Job<PairCompoundKeyValue, Double> job2 = jobTracker.newJob(source2);
        ICompletableFuture<List<Map.Entry<String, PairCompoundKeyValue>>> future2 = job2
                .mapper( new Key1PairMapper() )
                .reducer( new MaxValueReducerFactory())
                .submit(new KAscCollator<>());

        List<Map.Entry<String, PairCompoundKeyValue>> result2 = future2.get();
        // TODO write stop time

        //todo to output
        for (Map.Entry<String, PairCompoundKeyValue> e:result2) {
            System.out.println(" " + e.getValue().getK1() + " " + e.getValue().getK2() + " " + String.format(Locale.ROOT,"%.2f",e.getValue().getValue()));
        }

        this.instance.shutdown();
    }




}