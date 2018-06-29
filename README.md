Generic Jackson Serde for Kafka 
=========

For when you are working with Kafka and you just want to set your own instance of a configured
Jackson ObjectMapper for your Serializers/Deserializers.  (Useful when you are using Jackson binary
dataformats or special Jackson ObjectMapper configurations.)

Or you don't feel like creating your own version of this even though there are code examples out there
because you likely need something like this in your Kafka consumer/producer/streams projects that need
to work with JSON.

It's not anything revolutionary, but I got sick of seeing people creating customized versions of this type 
of thing for every project or worse for every type they deal with.  Maybe it's useful for you to have a 
premade version of this type of thing, too.

This pretty much does the same thing, so you should probably use it instead: [spring-kafka](http://projects.spring.io/spring-kafka/)'s [JsonSerializer](https://docs.spring.io/spring-kafka/api/org/springframework/kafka/support/serializer/JsonSerializer.html)/[JsonDeserializer](https://docs.spring.io/spring-kafka/api/org/springframework/kafka/support/serializer/JsonDeserializer.html)/[JsonSerde](https://docs.spring.io/spring-kafka/api/org/springframework/kafka/support/serializer/JsonSerde.html) I didn't know it existed at the time I wrote this library.

# Maven Dependency

```xml
<dependency>
  <groupId>com.giladam</groupId>
  <artifactId>kafka-jackson-serde</artifactId>
  <version>1.0.0</version>
</dependency>
```

# Example Usage in Java Code

Example usage for Kafka producer/consumer:

```java
    //An instance of a Jackson ObjectMapper configured to your liking: 
    ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(SerializationFeature.INDENT_OUTPUT)
            .findAndRegisterModules();

    //Using the serializer with a specific instance of an ObjectMapper:
    Producer<String,Sample> producer = new KafkaProducer<>(producerConfig,
                                                           Serdes.String().serializer(),
                                                           new Jackson2Serializer<>(OBJECT_MAPPER));
                                                           
    //Using the deserializer with a specific instance of an ObjectMapper and configuring to deserialize a particular type:
    KafkaConsumer<String,Sample> consumer = new KafkaConsumer<>(consumerConfig, 
                                                                Serdes.String().deserializer(),
                                                                new Jackson2Deserializer<>(OBJECT_MAPPER, Sample.class));
```

Example usage for Kafka Streams application:

```java
    //An instance of a Jackson ObjectMapper configured to your liking:
    ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .enable(SerializationFeature.INDENT_OUTPUT)
            .findAndRegisterModules();

    StreamsBuilder builder = new StreamsBuilder();

    //Using Serde with a specific instance of an ObjectMapper and configuring to handle a particular type:
    Serde<Sample> sampleSerde = new Jackson2Serde<>(OBJECT_MAPPER, Sample.class);
    Consumed<String, Sample> consumedWith = Consumed.with(Serdes.String(), sampleSerde);

    KStream<String, Sample> sampleMessages = builder.stream(TEST_TOPIC_NAME, consumedWith);
    
    ... the rest of your application...
```

# Running the Integration Tests

The integration tests require an actual Kafka Cluster to run.  They assume that this cluster is available at testing.kafka.host:9029 (which I configure by /etc/hosts/properties on my build machine.)
