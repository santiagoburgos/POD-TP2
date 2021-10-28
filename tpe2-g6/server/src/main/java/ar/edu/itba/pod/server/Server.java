package ar.edu.itba.pod.server;

import com.hazelcast.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.hazelcast.core.Hazelcast;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;

public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);

    public static void main(String[] args) throws IOException {
        logger.info("tpe2-g6 Server Starting ...");

        String ip = "192.168.1.*";
        try {
            Properties props = new Properties();
            String propFile = "config.properties";
            InputStream inputStream = Server.class.getClassLoader().getResourceAsStream(propFile);
            if (inputStream == null)
                throw new IllegalArgumentException();
            props.load(inputStream);
            ip = props.getProperty("ip");

        } catch (IllegalArgumentException ignored) {

        }
        logger.info("Using ip: " + ip);

        // Config
        Config config = new Config();

        // Group Config
        GroupConfig groupConfig = new GroupConfig().setName("g6").setPassword("g6-pass");
        config.setGroupConfig(groupConfig);

        // Network Config
        MulticastConfig multicastConfig = new MulticastConfig();
        JoinConfig joinConfig = new JoinConfig().setMulticastConfig(multicastConfig);

        InterfacesConfig interfacesConfig = new InterfacesConfig()
                .setInterfaces(Collections.singletonList(ip))
                .setEnabled(true);

        NetworkConfig networkConfig = new NetworkConfig()
                .setInterfaces(interfacesConfig)
                .setJoin(joinConfig);

        config.setNetworkConfig(networkConfig);

        // Management Center Config
        //ManagementCenterConfig managementCenterConfig = new ManagementCenterConfig()
        //        .setUrl("http://localhost:32768/mancenter/")
        //        .setEnabled(true);
        //config.setManagementCenterConfig(managementCenterConfig);


        // Start cluster
        Hazelcast.newHazelcastInstance(config);



    }
}
