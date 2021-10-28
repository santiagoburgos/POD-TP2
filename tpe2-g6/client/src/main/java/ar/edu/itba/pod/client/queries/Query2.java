package ar.edu.itba.pod.client.queries;


import ar.edu.itba.pod.api.collators.KAscCollator;

import ar.edu.itba.pod.api.mappers.CountOverValueMapper;
import ar.edu.itba.pod.api.mappers.Key1PairMapper;
import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.PairCompoundKeyValue;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.predicates.KeyInArrayPredicate;
import ar.edu.itba.pod.api.reducers.MaxValueReducerFactory;
import ar.edu.itba.pod.api.reducers.SumReducerFactory;

import ar.edu.itba.pod.client.EventType;
import ar.edu.itba.pod.client.TimeLogger;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query2 extends Query{
    private static final String QUERY_ID = "g6q2";


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
        logger.info("tpe2-g6 Query 2 Client Starting ...");

        TimeLogger timeLogger = new TimeLogger(QUERY_ID, this.outPath + "/time2.txt");

        // Parse arguments
        readArguments();

        // Config hazel
        configHazelCast();

        // Parse data
        timeLogger.addEvent(EventType.FILE_READ_START);
        List<Tree> trees = getTrees();
        List<String> neighbourhoods = getNeighbourhoods().stream().map(Neighbourhood::getName).collect(Collectors.toList());
        timeLogger.addEvent(EventType.FILE_READ_END);

        String imapName1 = "g6q21";
        IMap<String, PairCompoundKeyValue> treeOnNeighbourhood = this.instance.getMap(imapName1);
        treeOnNeighbourhood.clear();
        for (Tree t: trees) {
            treeOnNeighbourhood.put(t.getNeighbourhood().getName(), new PairCompoundKeyValue(t.getNeighbourhood().getName(),t.getName(), t.getNeighbourhood().getPopulation().doubleValue()) );
        }

        KeyValueSource<String, PairCompoundKeyValue> source = KeyValueSource.fromMap(treeOnNeighbourhood);
        JobTracker jobTracker = this.instance.getJobTracker(imapName1);


        Job<String, PairCompoundKeyValue> job = jobTracker.newJob(source);
        timeLogger.addEvent(EventType.MAPREDUCE_START);
        ICompletableFuture<Map<PairCompoundKeyValue, Double>> future = job
                .keyPredicate(new KeyInArrayPredicate(neighbourhoods))
                .mapper( new CountOverValueMapper<>() )
                .reducer( new SumReducerFactory())
                .submit();

        Map<PairCompoundKeyValue, Double> result = future.get();

        String imapName2 = "g6q22";
        IMap<PairCompoundKeyValue, Double> resImap = this.instance.getMap(imapName2);
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

        //todo to output
        for (Map.Entry<String, PairCompoundKeyValue> e:result2) {
            System.out.println(" " + e.getValue().getK1() + " " + e.getValue().getK2() + " " + String.format(Locale.ROOT,"%.2f",e.getValue().getValue()));
        }

        timeLogger.addEvent(EventType.MAPREDUCE_END);

        this.instance.shutdown();
    }




}