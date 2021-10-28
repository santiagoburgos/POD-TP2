package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.model.Neighbourhood;
import ar.edu.itba.pod.api.model.PairedValues;
import ar.edu.itba.pod.api.model.Tree;
import ar.edu.itba.pod.client.queries.Query5;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.KeyValueSource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public abstract class QueryTest {
    private TestHazelcastFactory hazelcastFactory;
    protected HazelcastInstance member, client;

    protected final List<Neighbourhood> neighbourhoods = Arrays.asList(
            new Neighbourhood("B1"), new Neighbourhood("B2"), new Neighbourhood("B3"));

    @Before
    public void setUp() {
        hazelcastFactory = new TestHazelcastFactory();

        // Group config
        GroupConfig groupConfig = new GroupConfig().setName("g6").setPassword("g6-pass");

        // Config
        Config config = new Config().setGroupConfig(groupConfig);

        member = hazelcastFactory.newHazelcastInstance(config);

        // Client config
        ClientConfig clientConfig = new ClientConfig().setGroupConfig(groupConfig);

        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public abstract void testQuery() throws ExecutionException, InterruptedException;

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }
}
