package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.model.Neighbourhood;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public abstract class QueryTest {
    private TestHazelcastFactory hazelcastFactory;
    protected HazelcastInstance member, client;

    protected final List<Neighbourhood> neighbourhoods = Arrays.asList(
            new Neighbourhood("B1", 2000L),
            new Neighbourhood("B2", 20000L),
            new Neighbourhood("B3", 31000L),
            new Neighbourhood("B4", 25500L),
            new Neighbourhood("B5", 200210L),
            new Neighbourhood("B6", 312000L),
            new Neighbourhood("B7", 45000L),
            new Neighbourhood("B8", 1122000L),
            new Neighbourhood("B9", 231000L),
            new Neighbourhood("B10", 12300L));

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
