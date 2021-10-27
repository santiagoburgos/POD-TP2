package ar.edu.itba.pod.client;

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
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import ar.edu.itba.pod.api.mappers.CounterMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.dsig.keyinfo.KeyValue;
import java.io.IOException;
import java.security.Key;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class TreesPerNeighborhood {

    private static Logger logger = LoggerFactory.getLogger(TreesPerNeighborhood.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        logger.info("tpe2-g6 Query 1 Client Starting ...");


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


        String ilistName = "g6q1";
        IList<String> treeOnNeighbourhood = hazelcastInstance.getList(ilistName);
        for (Tree t: trees) {
           if( neighbourhoods.contains(t.getNeighbourhood()) )
                treeOnNeighbourhood.add(t.getNeighbourhood().getName());
        }

        KeyValueSource<String,String> source = KeyValueSource.fromList(treeOnNeighbourhood);


        JobTracker jobTracker = hazelcastInstance.getJobTracker("g6q1");

        Job<String, String> job = jobTracker.newJob(source);
        ICompletableFuture<List<Map.Entry<String, Long>>> future = job
                .mapper( new CounterMapper() )
                .reducer( new SumReducerFactory())
                .submit(new DesVAscKCollator());


        List<Map.Entry<String, Long>> result = future.get();


        //todo to outfile
        for (Map.Entry<String, Long> e:result) {
            System.out.println("k " + e.getKey() + " v " + e.getValue());
        }


        HazelcastClient.shutdown(hazelcastInstance);
    }
}
