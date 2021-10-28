package ar.edu.itba.pod.client.queries;


import ar.edu.itba.pod.api.mappers.CountOverValueMapper;
import ar.edu.itba.pod.api.mappers.Key1PairMapper;
import ar.edu.itba.pod.api.model.PairCompoundKeyValue;

import ar.edu.itba.pod.api.collators.KAscCollator;

import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.Tree;

import ar.edu.itba.pod.api.predicates.KeyInArrayPredicate;
import ar.edu.itba.pod.api.reducers.MaxValueReducerFactory;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TempQ2 extends Query{

    private static Logger logger = LoggerFactory.getLogger(Query2.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {
        try {
            new TempQ2().run();
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

        // FileParser fp = new FileParser();
        // List<Neighbourhood> neighbourhoods = fp.parseNeighbourhoods(inPath, city);
        // List<Tree> trees = fp.parseTrees(inPath, city);

        List<Neighbourhood> neighbourhoods = new ArrayList<>();
        //neighbourhoods.add(new Neighbourhood("n3", 50l));
        neighbourhoods.add(new Neighbourhood("n1", 10l));
        neighbourhoods.add(new Neighbourhood("n2", 20l));

        List<String> neighbourhoods2 = new ArrayList<>();
        neighbourhoods2.add("n1");
        neighbourhoods2.add("n2");
        neighbourhoods2.add("n3");

        List<Tree> trees = new ArrayList<>();
        trees.add(new Tree(new Neighbourhood("n3", 50l), "s1", "t8" ));
        trees.add(new Tree(new Neighbourhood("n2", 20l), "s1", "t3" ));
        trees.add(new Tree(new Neighbourhood("n2", 20l), "s1", "t3" ));
        trees.add(new Tree(new Neighbourhood("n1", 10l), "s1", "t1" ));
        trees.add(new Tree(new Neighbourhood("n1", 10l), "s1", "t1" ));
        trees.add(new Tree(new Neighbourhood("n1", 10l), "s1", "t1" ));
        trees.add(new Tree(new Neighbourhood("n1", 10l), "s1", "t1" ));
        trees.add(new Tree(new Neighbourhood("n2", 20l), "s1", "t1" ));
        trees.add(new Tree(new Neighbourhood("n1", 10l), "s1", "t2" ));
        trees.add(new Tree(new Neighbourhood("n1", 10l), "s1", "t2" ));
        trees.add(new Tree(new Neighbourhood("n1", 10l), "s1", "t1" ));
        trees.add(new Tree(new Neighbourhood("n2", 20l), "s1", "t2" ));
        trees.add(new Tree(new Neighbourhood("n2", 20l), "s1", "t3" ));
        //





        String imapName1 = "g6q21";
        IMap<String, PairCompoundKeyValue> treeOnNeighbourhood = hazelcastInstance.getMap(imapName1);
        treeOnNeighbourhood.clear();
        for (Tree t: trees) {
            //if( neighbourhoods.contains(t.getNeighbourhood()) )
                treeOnNeighbourhood.put(t.getNeighbourhood().getName(), new PairCompoundKeyValue(t.getNeighbourhood().getName(),t.getName(), t.getNeighbourhood().getPopulation().doubleValue()) );
        }

        KeyValueSource<String, PairCompoundKeyValue> source = KeyValueSource.fromMap(treeOnNeighbourhood);
        JobTracker jobTracker = hazelcastInstance.getJobTracker(imapName1);


        Job<String, PairCompoundKeyValue> job = jobTracker.newJob(source);
        ICompletableFuture<Map<PairCompoundKeyValue, Double>> future = job
                .keyPredicate(new KeyInArrayPredicate(neighbourhoods2))
                .mapper( new CountOverValueMapper<>() )
                .reducer( new SumReducerFactory())
                .submit();

        Map<PairCompoundKeyValue, Double> result = future.get();

        String imapName2 = "g6q22";
        IMap<PairCompoundKeyValue, Double> resImap = hazelcastInstance.getMap(imapName2);
        resImap.clear();
        for (PairCompoundKeyValue t: result.keySet()) {
            resImap.put(t, result.get(t));
        }

        KeyValueSource<PairCompoundKeyValue, Double> source2 = KeyValueSource.fromMap(resImap);
        Job<PairCompoundKeyValue, Double> job2 = jobTracker.newJob(source2);
        ICompletableFuture<List<Map.Entry<String, PairCompoundKeyValue>>> future2 = job2
                .mapper( new Key1PairMapper() )
                .reducer( new MaxValueReducerFactory())
                .submit(new KAscCollator<>());

        List<Map.Entry<String, PairCompoundKeyValue>> result2 = future2.get();


        //todo to output
        for (Map.Entry<String, PairCompoundKeyValue> e:result2) {
            System.out.println(" " +  e.getValue().getK1() + " " + e.getValue().getK2() + " " + String.format(Locale.ROOT,"%.2f",e.getValue().getValue()));
        }

        HazelcastClient.shutdown(hazelcastInstance);
    }


}