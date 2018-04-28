package com.giladam.kafka.jacksonserde;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Jackson2Serializer<T> implements Serializer<T> {

    private final ObjectMapper objectMapper;

    public Jackson2Serializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Jackson2Serializer() {
        this(new ObjectMapper());
    }


    @Override
    public byte[] serialize(String topic, T objectToSerialize) {
        if (objectToSerialize == null) {
            return null;
        }

        try {
            return objectMapper.writeValueAsBytes(objectToSerialize);
        } catch (Exception e) {
            throw new SerializationException("Error serializing message", e);
        }
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}


    @Override
    public void close() {}
}
