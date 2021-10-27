package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.OutTreeNeighbourhood;
import ar.edu.itba.pod.api.collators.MaxTreePerPersonKDescCollator;
import ar.edu.itba.pod.api.mappers.CounterOverPopulationMapper;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TreesPerPerson {

    private static Logger logger = LoggerFactory.getLogger(TreesPerPerson.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        logger.info("tpe2-g6 Query 2 Client Starting ...");


        String addressesArg = Optional.ofNullable(System.getProperty("addresses")).orElseThrow(() -> new IllegalArgumentException("'addresses' argument needed."));
        String city = Optional.ofNullable(System.getProperty("city")).orElseThrow(() -> new IllegalArgumentException("'city' argument needed."));
        String inPath = Optional.ofNullable(System.getProperty("inPath")).orElseThrow(() -> new IllegalArgumentException("'inPath' argument needed."));
        String outPath = Optional.ofNullable(System.getProperty("outPath")).orElseThrow(() -> new IllegalArgumentException("'outPath' argument needed."));


        // Client Config
        ClientConfig clientConfig = new ClientConfig();

        // Group Config
        GroupConfig groupConfig = new GroupConfig().setName("g6").setPassword("g6-pass");
        clientConfig.setGroupConfig(groupConfig);

        // Client Network Config
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();


        String[] addresses = addressesArg.split(";");


        clientNetworkConfig.addAddress(addresses);
        clientConfig.setNetworkConfig(clientNetworkConfig);

        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

        FileParser fp = new FileParser();
        List<Neighbourhood> neighbourhoods = fp.parseNeighbourhoods(inPath, city);
        List<Tree> trees = fp.parseTrees(inPath, city);


        String ilistName = "g6q2";
        IList<OutTreeNeighbourhood> treeOnNeighbourhood = hazelcastInstance.getList(ilistName);
        treeOnNeighbourhood.clear();
        for (Tree t: trees) {
            if( neighbourhoods.contains(t.getNeighbourhood()) )
                treeOnNeighbourhood.add( new OutTreeNeighbourhood(t.getNeighbourhood().getName(),t.getName(), t.getNeighbourhood().getPopulation().doubleValue()) );
        }

        KeyValueSource<String, OutTreeNeighbourhood> source = KeyValueSource.fromList(treeOnNeighbourhood);

        JobTracker jobTracker = hazelcastInstance.getJobTracker(ilistName);

        Job<String, OutTreeNeighbourhood> job = jobTracker.newJob(source);
        ICompletableFuture<List<Map.Entry<OutTreeNeighbourhood, Double>>> future = job
                .mapper( new CounterOverPopulationMapper<>() )
                .reducer( new SumReducerFactory())
                .submit( new MaxTreePerPersonKDescCollator());

        List<Map.Entry<OutTreeNeighbourhood, Double>> result = future.get();


        //todo to outfile
        for (Map.Entry<OutTreeNeighbourhood, Double> e:result) {
            System.out.println(" " + e.getKey().getNeighbourhoodName() + " " + e.getKey().getTreeName() + " " + String.format(Locale.ROOT,"%.2f",e.getValue()) );
        }

        HazelcastClient.shutdown(hazelcastInstance);
    }
}