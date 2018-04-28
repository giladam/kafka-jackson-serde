package com.giladam.kafka.jacksonserde.streamstests;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

public class IntegrationTestingUtils {

    public static Properties getConfig() {
        Properties properties = new Properties();

        try (InputStream stream = Thread.currentThread()
                                        .getContextClassLoader()
                                        .getResourceAsStream("integration-test.properties")) {
            properties.load(stream);

            return properties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static AdminClient getAdminClient() {
        Map<String,Object> config = new HashMap<>();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersConfig());

        return AdminClient.create(config);
    }


    public static String bootstrapServersConfig() {
        Properties testingProps = IntegrationTestingUtils.getConfig();

        String testingKafkaHost = testingProps.getProperty("testing_kafka_hostname", "testing.kafka.host");
        String testingKafkaPort = testingProps.getProperty("9092", "9092");

        return testingKafkaHost + ":" + testingKafkaPort;
    }

}
