package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.api.collators.TopNCollator;
import ar.edu.itba.pod.api.mappers.NeighbourhoodSpeciesCounterMapper;
import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.reducers.UniqueReducerFactory;
import ar.edu.itba.pod.client.EventType;
import ar.edu.itba.pod.client.writers.Query3Writer;
import ar.edu.itba.pod.client.TimeLogger;
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

        Query3Writer queryWriter = new Query3Writer(this.outPath);
        TimeLogger timeLogger = new TimeLogger(QUERY_ID, this.outPath + "/time3.txt");

        // Parse arguments
        readArguments();

        // Config hazel
        configHazelCast();

        // Parse data
        timeLogger.addEvent(EventType.FILE_READ_START);
        List<Tree> trees = getTrees();
        List<String> neighbourhoods = getNeighbourhoods().stream().map(Neighbourhood::getName).collect(Collectors.toList());
        timeLogger.addEvent(EventType.FILE_READ_END);


        IList<Tree> dList = this.instance.getList(QUERY_ID + "l");
        dList.clear();
        dList.addAll(trees);

        // Get key value source
        KeyValueSource<String, Tree> source = KeyValueSource.fromList(dList);

        // Get job tracker
        JobTracker jobTracker = this.instance.getJobTracker(QUERY_ID + "j");

        Job<String, Tree> job = jobTracker.newJob(source);

        // Map reduce
        timeLogger.addEvent(EventType.MAPREDUCE_START);
        List<Map.Entry<String, Long>> result = mapReduce(neighbourhoods, job, this.n);
        timeLogger.addEvent(EventType.MAPREDUCE_END);

        queryWriter.writeQueryResults(result);

        // Shut down
        this.instance.shutdown();
    }

    // For testing
    public List<Map.Entry<String, Long>> mapReduce(List<String> neighbourhoods, Job<String, Tree> job, int n) throws ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<String, Long>>> future = job
                .mapper(new NeighbourhoodSpeciesCounterMapper(neighbourhoods))
                .reducer(new UniqueReducerFactory())
                .submit(new TopNCollator(n));

        return future.get();
    }
}
