package ar.edu.itba.pod.client;

import Collators.DesVAscKCollator;
import Reducers.SumReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import mappers.CounterMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class TreesPerNeighborhood {

    private static Logger logger = LoggerFactory.getLogger(TreesPerNeighborhood.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("tpe2-g6 Query 1 Client Starting ...");

        //todo parameters
        //String addressesArg = Optional.ofNullable(System.getProperty("addresses")).orElseThrow(() -> new IllegalArgumentException("'addresses' argument needed."));
        //String city = Optional.ofNullable(System.getProperty("city")).orElseThrow(() -> new IllegalArgumentException("'city' argument needed."));
        //String inPath = Optional.ofNullable(System.getProperty("inPath")).orElseThrow(() -> new IllegalArgumentException("'inPath' argument needed."));
        //String outPath = Optional.ofNullable(System.getProperty("outPath")).orElseThrow(() -> new IllegalArgumentException("'outPath' argument needed."));


        // Client Config
        ClientConfig clientConfig = new ClientConfig();

        // Group Config
        GroupConfig groupConfig = new GroupConfig().setName("g6").setPassword("g6-pass");
        clientConfig.setGroupConfig(groupConfig);

        // Client Network Config
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();

        //todo addresses parameter
        //String[] addresses = addressesArg.split(";");
        String[] addresses = {"192.168.0.220:5701"};


        clientNetworkConfig.addAddress(addresses);
        clientConfig.setNetworkConfig(clientNetworkConfig);

        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);


        //todo replace with data
        String mapName = "g6q1";
        IMap<String, String> testMap = hazelcastInstance.getMap(mapName);
        testMap.clear();
        testMap.set("a1", "ba");
        testMap.set("a2", "ba");
        testMap.set("a3", "bb");
        testMap.set("a4", "ba");
        testMap.set("a5", "bc");
        testMap.set("a6", "bc");
        testMap.set("a7", "bc");
        testMap.set("a8", "bf");
        testMap.set("a9", "bc");
        testMap.set("a10", "bd");

        KeyValueSource<String,String> source = KeyValueSource.fromMap(testMap);

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
