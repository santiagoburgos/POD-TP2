package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.api.collators.Query5Collator;
import ar.edu.itba.pod.api.combiners.Query5CombinerFactory;
import ar.edu.itba.pod.api.mappers.Query5Mapper;
import ar.edu.itba.pod.api.model.PairedValues;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.predicates.Query5Predicate;
import ar.edu.itba.pod.api.reducers.Query5ReducerFactory;
import ar.edu.itba.pod.client.exceptions.MissingFieldException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class Query5 extends Query {
    private static final String QUERY_ID = "g6q5";

    private static Logger logger = LoggerFactory.getLogger(Query5.class);

    private String neighbourhood;
    private String commonName;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try {
            new Query5().run();
        } catch (IOException e) {
            // TODO: do something
            logger.error(e.getMessage());
        }
    }

    @Override
    public void readArguments() {
        super.readArguments();
        this.neighbourhood = Optional.ofNullable(System.getProperty("neighbourhood")).orElseThrow(MissingFieldException::new);
        this.commonName = Optional.ofNullable(System.getProperty("commonName")).orElseThrow(MissingFieldException::new);
    }

    @Override
    public void run() throws IOException, ExecutionException, InterruptedException {
        logger.info("tpe2-g6 Query 5 Client Starting ...");

        // Parse arguments
        readArguments();

        // Config hazel
        configHazelCast();

        // Parse data
        List<Tree> trees = getTrees();
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
        ICompletableFuture<List<PairedValues>> completableFuture = job.keyPredicate(new Query5Predicate(this.neighbourhood))
                .mapper(new Query5Mapper(this.commonName))
                .combiner(new Query5CombinerFactory())
                .reducer(new Query5ReducerFactory())
                .submit(new Query5Collator());

        List<PairedValues> entries = completableFuture.get();
        // TODO write stop time

        // TODO write entries to csv

        // Shut down
        this.instance.shutdown();
    }

}
