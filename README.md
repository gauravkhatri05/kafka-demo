#Kafka

## Tutorials:
1. [https://medium.com/@patelharshali136/apache-kafka-tutorial-kafka-for-beginners-a58140cef84f](https://medium.com/@patelharshali136/apache-kafka-tutorial-kafka-for-beginners-a58140cef84f)
2. [https://kafka.apache.org/quickstart](https://kafka.apache.org/quickstart)
3. [https://kafka.apache.org/intro](https://kafka.apache.org/intro)
4. [https://dzone.com/articles/what-is-kafka](https://dzone.com/articles/what-is-kafka)
5. [https://dzone.com/articles/introduction-to-apache-kafka-1](https://dzone.com/articles/introduction-to-apache-kafka-1)
6. [https://dzone.com/articles/kafka-streams-more-than-just-dumb-storage](https://dzone.com/articles/kafka-streams-more-than-just-dumb-storage)
7. [https://dzone.com/articles/kafka-architecture](https://dzone.com/articles/kafka-architecture)
8. [https://dzone.com/articles/kafka-topic-architecture-replication-failover-and](https://dzone.com/articles/kafka-topic-architecture-replication-failover-and)
9. [https://dzone.com/articles/kafka-producer-architecture-picking-the-partition](https://dzone.com/articles/kafka-producer-architecture-picking-the-partition)
10. [https://dzone.com/articles/kafka-consumer-architecture-consumer-groups-and-su](https://dzone.com/articles/kafka-consumer-architecture-consumer-groups-and-su)
11. [https://www.youtube.com/watch?v=daRykH67_qs](https://www.youtube.com/watch?v=daRykH67_qs)
12. [https://www.youtube.com/watch?v=R873BlNVUB4](https://www.youtube.com/watch?v=R873BlNVUB4)
13. [https://www.baeldung.com/spring-kafka](https://www.baeldung.com/spring-kafka)
14. [https://howtodoinjava.com/kafka/spring-boot-with-kafka/](https://howtodoinjava.com/kafka/spring-boot-with-kafka/)

##How to add consumers into ConsumerGroup:
###[Reference doc](https://stackoverflow.com/questions/62673586/how-to-create-multiple-consumers-in-a-consume-group-using-spring-provided-kafka)

    e.g.
    @Configuration
    public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    String BOOTSTRAP_ADDRESS;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {

        final Map<String, Object> props = new HashMap<>();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_ADDRESS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, String>> concurrentKafkaListenerContainerFactory() {

        final ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(10);
        factory.getContainerProperties().setPollTimeout(3000);

        return factory;
    }

    