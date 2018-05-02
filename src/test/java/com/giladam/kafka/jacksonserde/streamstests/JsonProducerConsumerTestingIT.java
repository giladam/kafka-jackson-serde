package com.giladam.kafka.jacksonserde.streamstests;


import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.giladam.kafka.jacksonserde.Jackson2Deserializer;
import com.giladam.kafka.jacksonserde.Jackson2Serializer;
import com.giladam.kafka.jacksonserde.Sample;


public class JsonProducerConsumerTestingIT {

    private static final Logger log = LoggerFactory.getLogger(JsonProducerConsumerTestingIT.class);

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
        final String TEST_TOPIC_NAME = "kafka-jackson-serde.producer-consumer-tests.json";

        log.info("Creating testing topic: {}", TEST_TOPIC_NAME);
        NewTopic topicToCreate = new NewTopic(TEST_TOPIC_NAME, 1, (short) 1).configs(Collections.emptyMap());
        adminClient.createTopics(Collections.singleton(topicToCreate));

        Consumer<String,Sample> consumer = createConsumer();
        consumer.subscribe(Collections.singleton(TEST_TOPIC_NAME));

        sendMessagesToTopic(TEST_TOPIC_NAME);

        int maxPollsWithoutRecords = 10;
        int pollsWithoutRecords = 0;
        AtomicInteger numberOfRecordsRead = new AtomicInteger(0);

        while (pollsWithoutRecords <= maxPollsWithoutRecords) {
            ConsumerRecords<String, Sample> records = consumer.poll(1000);
            if (records.count() == 0) {
                pollsWithoutRecords++;
            } else {
                pollsWithoutRecords = 0;

                records.forEach(cr -> {
                    numberOfRecordsRead.incrementAndGet();
                    Assert.assertTrue(IntegrationTestingUtils.sampleHasNoNullFields(cr.value()));
                });
            }
        }

        log.info("Read {} records from topic", numberOfRecordsRead);
        Assert.assertTrue(numberOfRecordsRead.get() > 0);

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


    private Consumer<String,Sample> createConsumer() {
        Map<String,Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IntegrationTestingUtils.bootstrapServersConfig());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(consumerConfig,
                                   Serdes.String().deserializer(),
                                   new Jackson2Deserializer<>(OBJECT_MAPPER, Sample.class));
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
