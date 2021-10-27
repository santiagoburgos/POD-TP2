package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.collators.TopNCollator;
import ar.edu.itba.pod.api.mappers.NeighbourhoodSpeciesCounterMapper;
import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.reducers.UniqueReducerFactory;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public class TopNeighbourhoodDifferentSpecies {
    private static Logger logger = LoggerFactory.getLogger(TopNeighbourhoodDifferentSpecies.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        logger.info("tpe2-g6 Query 3 Client Starting ...");

//        String addressesParam = Optional.ofNullable(System.getProperty("addresses")).orElseThrow(() -> new IllegalArgumentException("'addresses' argument needed."));
//        String city = Optional.ofNullable(System.getProperty("city")).orElseThrow(() -> new IllegalArgumentException("'city' argument needed."));
//        String inPath = Optional.ofNullable(System.getProperty("inPath")).orElseThrow(() -> new IllegalArgumentException("'inPath' argument needed."));
//        String outPath = Optional.ofNullable(System.getProperty("outPath")).orElseThrow(() -> new IllegalArgumentException("'outPath' argument needed."));
//        String nStr = Optional.ofNullable(System.getProperty("n")).orElseThrow(() -> new IllegalArgumentException("'n' argument needed."));

        String addressesParam = "192.168.0.236:5701";
        String city = "VAN";
        String inPath = "/Users/gigi/Documents/ITBA/POD/csv";
        String outPath = "/Users/gigi/Documents/ITBA/POD/csv";
        String nStr = "5";
        int n = Integer.parseInt(nStr);

        if (n < 0) throw new IllegalArgumentException("n should be a positive integer");

        // Client Config
        ClientConfig clientConfig = new ClientConfig();

        // Group Config
        GroupConfig groupConfig = new GroupConfig().setName("g6").setPassword("g6-pass");
        clientConfig.setGroupConfig(groupConfig);

        // Client Network Config
        ClientNetworkConfig clientNetworkConfig = new ClientNetworkConfig();

        String[] addresses = addressesParam.split(";");
        //String[] addresses = {"192.168.0.220:5701"};

        clientNetworkConfig.addAddress(addresses);
        clientConfig.setNetworkConfig(clientNetworkConfig);

        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

//        FileParser fp = new FileParser();
////        List<Neighbourhood> neighbourhoods = fp.parseNeighbourhoods(inPath, city);
//        List<Tree> trees = fp.parseTrees(inPath, city);
//
//
//        String mapName = "g6q2";
//        IList<Tree> treeList = hazelcastInstance.getList(mapName);
//        treeList.addAll(trees);

        String mapName = "g6q3";
        IList<Tree> treeList = hazelcastInstance.getList(mapName);
        treeList.clear();
        treeList.add(new Tree("a1", "b1", "c1"));
        treeList.add(new Tree("a1", "b2", "c3"));
        treeList.add(new Tree("a1", "b3", "c2"));
        treeList.add(new Tree("a1", "b4", "c1"));
        treeList.add(new Tree("a2", "b5", "c1"));
        treeList.add(new Tree("a2", "b6", "c2"));
        treeList.add(new Tree("a3", "b7", "c1"));
        treeList.add(new Tree("a3", "b8", "c1"));


        //empezar a medir tiempo
        final JobTracker jobTracker = hazelcastInstance.getJobTracker(mapName);
        final KeyValueSource<String, Tree> source = KeyValueSource.fromList(treeList);
        final Job<String, Tree> job = jobTracker.newJob(source);

        ICompletableFuture<List<Map.Entry<String, Integer>>> future = job
                .mapper(new NeighbourhoodSpeciesCounterMapper())
                .reducer(new UniqueReducerFactory())
                .submit(new TopNCollator(n));

        List<Map.Entry<String, Integer>> result = future.get();

        for(Map.Entry<String, Integer> element: result) {
            System.out.println("Neighbourhood: " + element.getKey() + ", Diff Species: " + element.getValue());
        }

        //finalizar medir tiempo

    }


}
