package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.api.collators.DesVAscKCollator;
import ar.edu.itba.pod.api.mappers.TreesCounterMaper;
import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.reducers.SumReducerFactory;
import ar.edu.itba.pod.client.EventType;
import ar.edu.itba.pod.client.writers.Query1Writer;
import ar.edu.itba.pod.client.TimeLogger;
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
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class Query1 extends Query{

    private static final String QUERY_ID = "g6q1";

    private static Logger logger = LoggerFactory.getLogger(Query1.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try {
            new Query1().run();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

    }

    @Override
    public void run() throws IOException, ExecutionException, InterruptedException {
        logger.info("tpe2-g6 Query 1 Client Starting ...");

        Query1Writer queryWriter = new Query1Writer(this.outPath);
        TimeLogger timeLogger = new TimeLogger(QUERY_ID, this.outPath + "/time1.txt");

        // Parse arguments
        readArguments();

        // Config hazel
        configHazelCast();

        // Parse data
        timeLogger.addEvent(EventType.FILE_READ_START);
        List<Tree> trees = getTrees();
        List<String> neighbourhoods = getNeighbourhoods().stream().map(Neighbourhood::getName).collect(Collectors.toList());
        timeLogger.addEvent(EventType.FILE_READ_END);

        String ilistName = "g6q1";
        IList<String> treeOnNeighbourhood = this.instance.getList(ilistName);
        treeOnNeighbourhood.clear();
        for (Tree t: trees) {
                treeOnNeighbourhood.add( t.getNeighbourhood().getName());
        }

        KeyValueSource<String,String> source = KeyValueSource.fromList(treeOnNeighbourhood);
        JobTracker jobTracker = this.instance.getJobTracker(ilistName);

        Job<String, String> job = jobTracker.newJob(source);

        timeLogger.addEvent(EventType.MAPREDUCE_START);
        List<Map.Entry<String, Double>> result = mapReduce(job, neighbourhoods);
        timeLogger.addEvent(EventType.MAPREDUCE_END);

        queryWriter.writeQueryResults(result);

        this.instance.shutdown();
    }

    public List<Map.Entry<String, Double>> mapReduce(Job<String, String> job, List<String> neighbourhoods) throws ExecutionException, InterruptedException {
        ICompletableFuture<List<Map.Entry<String, Double>>> future = job
                .mapper( new TreesCounterMaper(neighbourhoods) )
                .reducer( new SumReducerFactory())
                .submit(new DesVAscKCollator());


        return future.get();
    }


}
