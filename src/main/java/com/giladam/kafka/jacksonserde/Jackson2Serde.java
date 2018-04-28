package com.giladam.kafka.jacksonserde;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class Jackson2Serde<T> implements Serde<T> {

    private Serializer<T> serializer;

    private Deserializer<T> deserializer;

    /**
     * Constructs a new Serde with a default Jackson ObjectMapper.
     * @param serdeForType
     *  Configures to deserialize the specified type.
     */
    public Jackson2Serde(Class<T> serdeForType) {
         this(new ObjectMapper(), serdeForType);
    }


    /**
     * Constructs a new Serde with an instance of a Jackson ObjectMapper.
     * @param objectMapper
     *  Specifies the ObjectMapper instance to use for all serializing and deserializing.
     * @param serdeForType
     *  Configures to deserialize the specified type.
     */
    public Jackson2Serde(ObjectMapper objectMapper, Class<T> serdeForType) {
         this.serializer = new Jackson2Serializer<>(objectMapper);
         this.deserializer = new Jackson2Deserializer<>(objectMapper, serdeForType);
    }


    @Override
    public Serializer<T> serializer() {
        return serializer;
    }


    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}


    @Override
    public void close() {}

}
