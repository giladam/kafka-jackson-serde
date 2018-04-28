package com.giladam.kafka.jacksonserde.streamstests;


import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.giladam.kafka.jacksonserde.Jackson2Serde;
import com.giladam.kafka.jacksonserde.Jackson2Serializer;
import com.giladam.kafka.jacksonserde.Sample;


public class StreamsApplicationTestingIT {

    private static final Logger log = LoggerFactory.getLogger(StreamsApplicationTestingIT.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(SerializationFeature.INDENT_OUTPUT)
            .findAndRegisterModules();


    private static AdminClient adminClient;

    @BeforeClass
    public static void setupOnce() {
        adminClient = IntegrationTestingUtils.getAdminClient();
    }


    @AfterClass
    public static void afterAllTests() {
        adminClient.close();
    }


    @Test
    public void testStreamsApplicationSerde() throws InterruptedException {
        final String TEST_TOPIC_NAME = "kafka-jackson-serde.streamstests";

        log.info("Creating testing topic: {}", TEST_TOPIC_NAME);
        NewTopic topicToCreate = new NewTopic(TEST_TOPIC_NAME, 1, (short) 1).configs(Collections.emptyMap());
        adminClient.createTopics(Collections.singleton(topicToCreate));

        Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        streamsConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, IntegrationTestingUtils.bootstrapServersConfig());
        streamsConfig.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        StreamsBuilder builder = new StreamsBuilder();

        Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, streamsConfig);

        Serde<Sample> sampleSerde = new Jackson2Serde<>(OBJECT_MAPPER, Sample.class);
        Consumed<String, Sample> consumedWith = Consumed.with(Serdes.String(), sampleSerde)
                                                        .withOffsetResetPolicy(Topology.AutoOffsetReset.EARLIEST);

        KStream<String, Sample> sampleMessages = builder.stream(TEST_TOPIC_NAME, consumedWith);

        AtomicBoolean somethingGotStreamed = new AtomicBoolean(false);

        //test if we deserialize what's in the stream correctly:
        sampleMessages.foreach((k,v) -> {
            log.info("Read {}", k);
            somethingGotStreamed.set(true);
            Assert.assertTrue(ObjectUtils.allNotNull(v.getDateField(),
                                                     v.getInstantField(),
                                                     v.getStringField()));
        });

        streams.cleanUp();
        streams.start();

        //give it some time to startup and run
        Thread.sleep(5000); //TODO: change to timeout

        sendMessagesToTopic(TEST_TOPIC_NAME);

        //give it some time to run
        Thread.sleep(5000);//TODO: change to timeout

        Assert.assertTrue(somethingGotStreamed.get());

        log.info("Shutting down.");
        streams.close();
        log.info("Shutdown complete.");

        log.info("Deleting testing topic: {}", TEST_TOPIC_NAME);
        adminClient.deleteTopics(Collections.singleton(TEST_TOPIC_NAME));
    }


    private void sendMessagesToTopic(String topicName) {
        Producer<String,Sample> producer = createProducer();

        for (int i=0; i<1000; i++) {
            producer.send(newProducerRecord(topicName));
        }
        producer.flush();
        producer.close();

        log.info("Sent messages to topic: {}", topicName);
    }


    private Producer<String,Sample> createProducer() {
        Map<String,Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IntegrationTestingUtils.bootstrapServersConfig());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        Producer<String,Sample> producer = new KafkaProducer<>(producerConfig,
                                                               Serdes.String().serializer(),
                                                               new Jackson2Serializer<>(OBJECT_MAPPER));
        return producer;
    }


    private ProducerRecord<String,Sample> newProducerRecord(String topic) {
        Sample sample = new Sample();

        Instant now = Instant.now();

        sample.setDateField(Date.from(now));
        sample.setStringField(UUID.randomUUID().toString());
        sample.setInstantField(now);

        ProducerRecord<String,Sample> record = new ProducerRecord<>(topic, sample.getStringField(), sample);
        return record;
    }

}
