package ar.edu.itba.pod.client.queries;

import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.client.FileParser;
import ar.edu.itba.pod.client.exceptions.MissingFieldException;
import ar.edu.itba.pod.client.exceptions.NoSuchCityException;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public abstract class Query {
    private final static String[] validCities = {"BUE", "VAN"};

    protected String city;
    protected final List<String> addressesList = new ArrayList<>();
    protected String inPath;
    protected String outPath;

    protected HazelcastInstance instance;

    private final FileParser parser = new FileParser();

    public void readArguments() {
        // Parse city
        this.city = Optional.ofNullable(System.getProperty("city")).orElseThrow(MissingFieldException::new);
        if (Arrays.stream(validCities).noneMatch(s -> s.equals(this.city))) throw new NoSuchCityException();

        // Parse addresses
        String addresses = Optional.ofNullable(System.getProperty("addresses")).orElseThrow(MissingFieldException::new);
        addressesList.clear();
        addressesList.addAll(Arrays.asList(addresses.split(";")));

        // Parse paths
        this.inPath = Optional.ofNullable(System.getProperty("inPath")).orElseThrow(MissingFieldException::new);
        this.outPath = Optional.ofNullable(System.getProperty("outPath")).orElseThrow(MissingFieldException::new);
    }

    public void configHazelCast() {
        final ClientConfig config = new ClientConfig();
        GroupConfig groupConfig = new GroupConfig().setName("g6").setPassword("g6-pass");
        config.setGroupConfig(groupConfig);
        config.getNetworkConfig().setAddresses(addressesList);
        this.instance = HazelcastClient.newHazelcastClient(config);
    }

    public List<Tree> getTrees() throws IOException {
        List<Tree> trees = parser.parseTrees(inPath, city);

        return trees;
    }

    public List<Neighbourhood> getNeighbourhoods() throws IOException {
        List<Neighbourhood> neighbourhoods = parser.parseNeighbourhoods(inPath, city);
        return neighbourhoods;
    }

    public abstract void run() throws IOException, ExecutionException, InterruptedException;
}
