package ar.edu.itba.pod.client.queries;


import ar.edu.itba.pod.api.collators.DesVAscKCollator;
import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.api.predicates.GenericKeyPredicate;
import ar.edu.itba.pod.api.predicates.KeyInArrayPredicate;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class TempQ1 extends Query{

    private static Logger logger = LoggerFactory.getLogger(Query1.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try {
            new TempQ1().run();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }

    }

    @Override
    public void run() throws IOException, ExecutionException, InterruptedException {
        logger.info("tpe2-g6 Query 1 Client Starting ...");


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


        //String[] addresses = addressesArg.split(";");
        String[] addresses = {"192.168.0.220"};

        clientNetworkConfig.addAddress(addresses);
        clientConfig.setNetworkConfig(clientNetworkConfig);

        HazelcastInstance hazelcastInstance = HazelcastClient.newHazelcastClient(clientConfig);

        ///FileParser fp = new FileParser();
        //List<Neighbourhood> neighbourhoods = fp.parseNeighbourhoods(inPath, city);
        //List<Tree> trees = fp.parseTrees(inPath, city);

        //
        List<Tree> trees = new ArrayList<>();
        trees.add(new Tree(new Neighbourhood("n1", 10l), "s1", "t1") );
        trees.add(new Tree(new Neighbourhood("n1", 10l), "s1", "t1") );
        trees.add(new Tree(new Neighbourhood("n1", 10l), "s1", "t2") );
        trees.add(new Tree(new Neighbourhood("n2", 20l), "s1", "t1") );
        List<Neighbourhood> neighbourhoods = new ArrayList<>();
        neighbourhoods.add(new Neighbourhood("n1", 10l));
        neighbourhoods.add(new Neighbourhood("n2", 20l));
        //

        List<String> neighbourhoods2 = new ArrayList<>();
        neighbourhoods2.add("n1");
        neighbourhoods2.add("n2");

        /*
        String ilistName = "g6q1";
        IList<String> treeOnNeighbourhood = hazelcastInstance.getList(ilistName);
        treeOnNeighbourhood.clear();
        for (Tree t: trees) {
            //if( neighbourhoods.contains(t.getNeighbourhood()) )
                treeOnNeighbourhood.add(t.getNeighbourhood().getName());
        }
         */
        String imapName = "g6q1";
        IMap<String, String> treeOnNeighbourhood = hazelcastInstance.getMap(imapName);
        treeOnNeighbourhood.clear();
        for (Tree t: trees) {
            //if( neighbourhoods.contains(t.getNeighbourhood()) )
            treeOnNeighbourhood.put(t.getNeighbourhood().getName(), t.getNeighbourhood().getName());
        }

        KeyValueSource<String,String> source = KeyValueSource.fromMap(treeOnNeighbourhood);


        JobTracker jobTracker = hazelcastInstance.getJobTracker("g6q1");

        Job<String, String> job = jobTracker.newJob(source);
        ICompletableFuture<List<Map.Entry<String, Double>>> future = job
                .keyPredicate(new KeyInArrayPredicate(neighbourhoods2))
                .mapper( new CounterMapper() )
                .reducer( new SumReducerFactory())
                .submit(new DesVAscKCollator());


        List<Map.Entry<String, Double>> result = future.get();


        //todo to outfile
        for (Map.Entry<String, Double> e:result) {
            System.out.println("k " + e.getKey() + " v " + e.getValue().longValue());
        }


        HazelcastClient.shutdown(hazelcastInstance);
    }


}
