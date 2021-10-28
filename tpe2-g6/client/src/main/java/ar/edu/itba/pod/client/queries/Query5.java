package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.api.collators.PairedValuesCollator;
import ar.edu.itba.pod.api.combiners.SumCombinerFactory;
import ar.edu.itba.pod.api.mappers.StreetSpecificTreeNameMapper;
import ar.edu.itba.pod.api.model.PairedValues;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.reducers.SumInTensReducerFactory;
import ar.edu.itba.pod.client.writers.EventType;
import ar.edu.itba.pod.client.writers.Query5Writer;
import ar.edu.itba.pod.client.writers.TimeLogger;
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

        // Writers
        Query5Writer queryWriter = new Query5Writer(this.outPath);
        TimeLogger timeLogger = new TimeLogger(QUERY_ID, this.outPath + "/time5.txt");

        // Config hazel
        configHazelCast();

        // Parse data
        timeLogger.addEvent(EventType.FILE_READ_START);
        List<Tree> trees = getTrees();
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
        List<PairedValues> entries = mapReduce(job, this.neighbourhood, this.commonName);
        timeLogger.addEvent(EventType.MAPREDUCE_END);

        queryWriter.writeQueryResults(entries);

        // Shut down
        this.instance.shutdown();
    }

    // For testing
    public List<PairedValues> mapReduce(Job<String, Tree> job, String neighbourhood, String commonName) throws ExecutionException, InterruptedException {
        ICompletableFuture<List<PairedValues>> completableFuture = job
                .mapper(new StreetSpecificTreeNameMapper(neighbourhood, commonName))
                .combiner(new SumCombinerFactory())
                .reducer(new SumInTensReducerFactory())
                .submit(new PairedValuesCollator());

        return completableFuture.get();
    }

}
