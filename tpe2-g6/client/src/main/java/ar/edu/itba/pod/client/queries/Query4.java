package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.api.collators.PairedValuesCollator;
import ar.edu.itba.pod.api.mappers.NeighbourhoodSpeciesCounterMapper;
import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.PairedValues;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.predicates.KeyInArrayPredicate;
import ar.edu.itba.pod.api.reducers.UniqueReducerFactory;
import ar.edu.itba.pod.client.EventType;
import ar.edu.itba.pod.client.writers.Query4Writer;
import ar.edu.itba.pod.client.TimeLogger;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query4 extends Query {
    private static final String QUERY_ID = "g6q4";

    private static Logger logger = LoggerFactory.getLogger(Query4.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try {
            new Query4().run();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    @Override
    public void run() throws IOException, ExecutionException, InterruptedException {
        logger.info("tpe2-g6 Query 4 Client Starting ...");

        Query4Writer queryWriter = new Query4Writer(this.outPath);
        TimeLogger timeLogger = new TimeLogger(QUERY_ID, this.outPath + "/time4.txt");

        // Parse arguments
        readArguments();

        // Config hazel
        configHazelCast();

        // Prepare data
        timeLogger.addEvent(EventType.FILE_READ_START);
        List<Tree> trees = getTrees();
        List<String> neighbourhoods = getNeighbourhoods().stream().map(Neighbourhood::getName).collect(Collectors.toList());
        timeLogger.addEvent(EventType.FILE_READ_END);

        IMap<String, Tree> dMap = this.instance.getMap(QUERY_ID + "m");
        dMap.clear();
        trees.forEach(tree -> dMap.put(tree.getNeighbourhood().getName(), tree));

        // Get key value source
        KeyValueSource<String, Tree> source = KeyValueSource.fromMap(dMap);

        // Get job tracker
        JobTracker jobTracker = this.instance.getJobTracker(QUERY_ID + "j");

        Job<String, Tree> job = jobTracker.newJob(source);

        // Map reduce
        timeLogger.addEvent(EventType.MAPREDUCE_START);
        ICompletableFuture<List<PairedValues>> completableFuture = job
                .keyPredicate(new KeyInArrayPredicate(neighbourhoods))
                .mapper(new NeighbourhoodSpeciesCounterMapper())
                .reducer(new UniqueReducerFactory(100L))
                .submit(new PairedValuesCollator());

        List<PairedValues> entries = completableFuture.get();

        queryWriter.writeQueryResults(entries);
        timeLogger.addEvent(EventType.MAPREDUCE_END);

        // Shut down
        this.instance.shutdown();
    }
}
