package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.api.collators.PairedValuesCollator;
import ar.edu.itba.pod.api.mappers.NeighbourhoodSpeciesCounterMapper;
import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.PairedValues;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.predicates.KeyInArrayPredicate;
import ar.edu.itba.pod.api.reducers.UniqueReducerFactory;
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

        // Parse arguments
        readArguments();

        // Config hazel
        configHazelCast();

        // Prepare data
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
        // TODO write start time
        ICompletableFuture<List<PairedValues>> completableFuture = job
                .keyPredicate(new KeyInArrayPredicate(neighbourhoods))
                .mapper(new NeighbourhoodSpeciesCounterMapper())
                .reducer(new UniqueReducerFactory(100L))
                .submit(new PairedValuesCollator());

        List<PairedValues> entries = completableFuture.get();
        // TODO write stop time

        // TODO write entries to csv

        // Shut down
        this.instance.shutdown();
    }
}
