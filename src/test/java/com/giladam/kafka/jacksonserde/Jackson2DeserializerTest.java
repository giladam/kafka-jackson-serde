package com.giladam.kafka.jacksonserde;

import java.time.Instant;
import java.util.Date;
import java.util.UUID;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class Jackson2DeserializerTest {

    @Test
    public void testDeserialize() throws JsonProcessingException {
        Sample sample = new Sample();

        Instant now = Instant.now();

        sample.setDateField(Date.from(now));
        sample.setStringField(UUID.randomUUID().toString());
        sample.setInstantField(now);

        ObjectMapper mapper = new ObjectMapper()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.INDENT_OUTPUT)
                .findAndRegisterModules();

        byte[] serializedBytes = mapper.writeValueAsBytes(sample);

        try (Jackson2Deserializer<Sample> deserializer = new Jackson2Deserializer<>(mapper, Sample.class)) {
            Sample sampleDeserialized = deserializer.deserialize("mock-topic", serializedBytes);

            boolean same = EqualsBuilder.reflectionEquals(sample, sampleDeserialized, true);
            Assert.assertTrue(same);
        }
    }


    @Test
    public void testDeserialize_NullHandling() throws JsonProcessingException {
        try (Jackson2Deserializer<Sample> deserializer = new Jackson2Deserializer<>(Sample.class)) {
            Sample sampleDeserialized = deserializer.deserialize("mock-topic", null);
            Assert.assertNull(sampleDeserialized);
        }
    }


    @Test(expected=SerializationException.class)
    public void testDeserialize_ThrowsSerializationExceptionOnBadData() throws JsonProcessingException {
        try (Jackson2Deserializer<Sample> deserializer = new Jackson2Deserializer<>(Sample.class)) {
            deserializer.deserialize("mock-topic", "THIS IS BAD DATA".getBytes());
            Assert.fail("Exception should have been thrown before getting here.");
        }
    }
}
