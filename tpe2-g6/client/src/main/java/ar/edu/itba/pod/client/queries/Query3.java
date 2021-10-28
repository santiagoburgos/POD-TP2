package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.api.collators.TopNCollator;
import ar.edu.itba.pod.api.mappers.NeighbourhoodSpeciesCounterMapper;
import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.predicates.KeyInArrayPredicate;
import ar.edu.itba.pod.api.reducers.UniqueReducerFactory;
import ar.edu.itba.pod.client.EventType;
import ar.edu.itba.pod.client.TimeLogger;
import ar.edu.itba.pod.client.exceptions.MissingFieldException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query3 extends Query{
    private static final String QUERY_ID = "g6q3";

    private static Logger logger = LoggerFactory.getLogger(Query3.class);

    private int n;

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try {
            new Query3().run();
        } catch (IOException e) {
            // TODO: do something
            logger.error(e.getMessage());
        }

    }

    @Override
    public void readArguments() {
        super.readArguments();
        String nString= Optional.ofNullable(System.getProperty("n")).orElseThrow(MissingFieldException::new);
        n = Integer.parseInt(nString);

        if (n < 0) throw new IllegalArgumentException("n should be a positive integer");
    }

    @Override
    public void run() throws IOException, ExecutionException, InterruptedException {
        logger.info("tpe2-g6 Query 3 Client Starting ...");

        TimeLogger timeLogger = new TimeLogger(QUERY_ID, this.outPath + "/time3.txt");

        // Parse arguments
        readArguments();

        // Config hazel
        configHazelCast();

        // Parse data
        List<Tree> trees = getTrees();
        List<String> neighbourhoods = getNeighbourhoods().stream().map(Neighbourhood::getName).collect(Collectors.toList());


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

        ICompletableFuture<List<Map.Entry<String, Long>>> future = job
                .keyPredicate(new KeyInArrayPredicate(neighbourhoods))
                .mapper(new NeighbourhoodSpeciesCounterMapper())
                .reducer(new UniqueReducerFactory())
                .submit(new TopNCollator(n));

        List<Map.Entry<String, Long>> result = future.get();

        // TODO write entries to csv
        timeLogger.addEvent(EventType.MAPREDUCE_END);

        // Shut down
        this.instance.shutdown();
    }
}
