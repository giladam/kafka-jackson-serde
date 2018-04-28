package com.giladam.kafka.jacksonserde;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class Jackson2SerializerTest {

    private static final Logger log = LoggerFactory.getLogger(Jackson2SerializerTest.class);

    @Test
    public void testSerializerUsesConfiguredMapper_DatesAsTimestamps() {

        ObjectMapper mapper = new ObjectMapper()
                .enable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.INDENT_OUTPUT)
                .findAndRegisterModules();

        Jackson2Serializer<Sample> sampleSerializer = new Jackson2Serializer<>(mapper);
        sampleSerializer.configure(Collections.emptyMap(), false);

        ProducerRecord<String,Sample> sampleRecord = newProducerRecord();

        byte[] jsonBytes = sampleSerializer.serialize(sampleRecord.topic(), sampleRecord.value());
        String jsonString = new String(jsonBytes, StandardCharsets.UTF_8);

        log.info("Serialized to: {}", jsonString);

        sampleSerializer.close();
    }


    @Test
    public void testSerializerUsesConfiguredMapper_DatesAsISO8601() {
        ObjectMapper mapper = new ObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.INDENT_OUTPUT)
                .findAndRegisterModules();

        Jackson2Serializer<Sample> sampleSerializer = new Jackson2Serializer<>(mapper);
        sampleSerializer.configure(Collections.emptyMap(), false);

        ProducerRecord<String,Sample> sampleRecord = newProducerRecord();

        byte[] jsonBytes = sampleSerializer.serialize(sampleRecord.topic(), sampleRecord.value());
        String jsonString = new String(jsonBytes, StandardCharsets.UTF_8);

        log.info("Serialized to: {}", jsonString);

        sampleSerializer.close();
    }


    @Test
    public void testSerializerSerializingNullReturnsNull() {
        Jackson2Serializer<Sample> sampleSerializer = new Jackson2Serializer<>();
        sampleSerializer.configure(Collections.emptyMap(), false);

        byte[] jsonBytes = sampleSerializer.serialize("mocktopic", null);
        Assert.assertNull(jsonBytes);

        sampleSerializer.close();
    }


    private ProducerRecord<String,Sample> newProducerRecord() {
        Sample sample = new Sample();

        Instant now = Instant.now();

        sample.setDateField(Date.from(now));
        sample.setStringField(UUID.randomUUID().toString());
        sample.setInstantField(now);

        ProducerRecord<String,Sample> record = new ProducerRecord<>("mock", sample.getStringField(), sample);
        return record;
    }

}
