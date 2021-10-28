package ar.edu.itba.pod.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.HazelcastInstance;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HazelcastTest {

    private TestHazelcastFactory hazelcastFactory;
    private HazelcastInstance member, client;

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
    public void testQuery1() {

    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }
}
