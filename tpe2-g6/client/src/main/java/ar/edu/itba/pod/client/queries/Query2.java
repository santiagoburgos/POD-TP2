package ar.edu.itba.pod.client.queries;


import ar.edu.itba.pod.api.collators.KAscCollator;

import ar.edu.itba.pod.api.mappers.CountOverValueMapper;
import ar.edu.itba.pod.api.mappers.Key1PairMapper;
import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.PairCompoundKeyValue;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.reducers.MaxValueReducerFactory;
import ar.edu.itba.pod.api.reducers.SumReducerFactory;

import ar.edu.itba.pod.client.EventType;
import ar.edu.itba.pod.client.writers.Query2Writer;
import ar.edu.itba.pod.client.TimeLogger;
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

        Query2Writer queryWriter = new Query2Writer(this.outPath);
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

        String ilistName = "g6q21";
        IList<PairCompoundKeyValue> treeOnNeighbourhood = this.instance.getList(ilistName);
        treeOnNeighbourhood.clear();
        for (Tree t: trees) {
            treeOnNeighbourhood.add(new PairCompoundKeyValue(t.getNeighbourhood().getName(),t.getName(), t.getNeighbourhood().getPopulation().doubleValue()) );
        }

        KeyValueSource<String, PairCompoundKeyValue> source = KeyValueSource.fromList(treeOnNeighbourhood);
        JobTracker jobTracker = this.instance.getJobTracker(QUERY_ID);
        Job<String, PairCompoundKeyValue> job = jobTracker.newJob(source);

        timeLogger.addEvent(EventType.MAPREDUCE_START);

        Map<PairCompoundKeyValue, Double> result = mapReduce1(job, neighbourhoods);

        String imapName = "g6q22";
        IMap<PairCompoundKeyValue, Double> resImap = this.instance.getMap(imapName);
        resImap.clear();
        for (PairCompoundKeyValue t: result.keySet()) {
            resImap.put(t, result.get(t));
        }
        KeyValueSource<PairCompoundKeyValue, Double> source2 = KeyValueSource.fromMap(resImap);
        Job<PairCompoundKeyValue, Double> job2 = this.instance.getJobTracker(QUERY_ID).newJob(source2);

        List<Map.Entry<String, PairCompoundKeyValue>> result2 = mapReduce2(job2);

        timeLogger.addEvent(EventType.MAPREDUCE_END);
        queryWriter.writeQueryResults(result2);

        this.instance.shutdown();
    }

    public Map<PairCompoundKeyValue, Double> mapReduce1( Job<String, PairCompoundKeyValue> job, List<String> neighbourhoods) throws ExecutionException, InterruptedException {
        ICompletableFuture<Map<PairCompoundKeyValue, Double>> future = job
                .mapper( new CountOverValueMapper<>(neighbourhoods) )
                .reducer( new SumReducerFactory())
                .submit();

        return future.get();
    }


    public List<Map.Entry<String, PairCompoundKeyValue>> mapReduce2(Job<PairCompoundKeyValue, Double>  job) throws ExecutionException, InterruptedException {

        ICompletableFuture<List<Map.Entry<String, PairCompoundKeyValue>>> future2 = job
                .mapper( new Key1PairMapper() )
                .reducer( new MaxValueReducerFactory())
                .submit(new KAscCollator<>());

        return future2.get();
    }




}