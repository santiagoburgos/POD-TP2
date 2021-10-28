package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.api.collators.TopNCollator;
import ar.edu.itba.pod.api.mappers.NeighbourhoodSpeciesCounterMapper;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.reducers.UniqueReducerFactory;
import ar.edu.itba.pod.client.exceptions.MissingFieldException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
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

        // Parse arguments
        readArguments();

        // Config hazel
        configHazelCast();

        // Parse data
        List<Tree> trees = getTrees();
        IList<Tree> dlist = this.instance.getList(QUERY_ID + "l");
        dlist.addAll(trees);

        // Get key value source
        KeyValueSource<String, Tree> source = KeyValueSource.fromList(dlist);

        // Get job tracker
        JobTracker jobTracker = this.instance.getJobTracker(QUERY_ID + "j");

        Job<String, Tree> job = jobTracker.newJob(source);

        // Map reduce
        // TODO write start time

        ICompletableFuture<List<Map.Entry<String, Integer>>> future = job
                .mapper(new NeighbourhoodSpeciesCounterMapper())
                .reducer(new UniqueReducerFactory())
                .submit(new TopNCollator(n));

        List<Map.Entry<String, Integer>> result = future.get();

        // TODO write stop time

        // TODO write entries to csv

        // Shut down
        this.instance.shutdown();
    }
}
