package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.api.collators.DesVAscKCollator;
import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.reducers.SumReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import ar.edu.itba.pod.api.mappers.CounterMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Query1 extends Query{

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

        // Parse arguments
        readArguments();

        // Config hazel
        configHazelCast();

        // Parse data
        List<Neighbourhood> neighbourhoods = getNeighbourhoods();
        List<Tree> trees = getTrees();


        String ilistName = "g6q1";
        IList<String> treeOnNeighbourhood = this.instance.getList(ilistName);
        treeOnNeighbourhood.clear();
        for (Tree t: trees) {
           if( neighbourhoods.contains(t.getNeighbourhood()) )
                treeOnNeighbourhood.add(t.getNeighbourhood().getName());
        }

        KeyValueSource<String,String> source = KeyValueSource.fromList(treeOnNeighbourhood);

        JobTracker jobTracker = this.instance.getJobTracker("g6q1");

        Job<String, String> job = jobTracker.newJob(source);
        ICompletableFuture<List<Map.Entry<String, Double>>> future = job
                .mapper( new CounterMapper() )
                .reducer( new SumReducerFactory())
                .submit(new DesVAscKCollator());


        List<Map.Entry<String, Double>> result = future.get();


        //todo to outfile
        for (Map.Entry<String, Double> e:result) {
            System.out.println("k " + e.getKey() + " v " + e.getValue().longValue());
        }


        this.instance.shutdown();
    }


}
