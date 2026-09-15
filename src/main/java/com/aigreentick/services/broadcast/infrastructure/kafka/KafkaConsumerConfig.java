package com.aigreentick.services.broadcast.infrastructure.kafka;

import com.aigreentick.services.broadcast.common.constants.InfraConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Consumer wiring for the two inbound topics.
 *
 * <p>Two separate factories because the topics have opposite requirements. Dispatch needs manual
 * acknowledgement, since an offset may only move once every recipient in the batch is resolved.
 * Capacity is a compacted stream of current values where automatic acknowledgement is right and
 * replaying from the beginning on every start is exactly what is wanted.
 *
 * <h2>These factories are hand-built, so {@code spring.kafka.consumer.*} is ignored</h2>
 * Declaring {@link ConsumerFactory} beans makes Spring Boot back off its auto-configured one and
 * every property under {@code spring.kafka.consumer} with it. The group id and poll size in
 * {@code application.yml} reach this class only because {@link InfraConstants.ConfigKeys} reads
 * them through {@code @Value} and they are put into the map below by hand. Any other consumer
 * setting added to that YAML block does nothing at all.
 *
 * <h2>Fetch size has to track the producer's record size</h2>
 * A dispatch batch of 2,000 recipients is around 1.26 MB and the Messaging Service now sends up to
 * {@code max.request.size} of 10 MB. {@code max.partition.fetch.bytes} defaults to 1 MB, and a
 * record the broker accepted but a consumer cannot fetch does not fail — it stalls that partition
 * permanently, with the consumer retrying the same fetch forever and no error that names the
 * cause. That is a worse outcome than the rejected send it replaces, which is why this value must
 * be raised whenever the producer's is, and why it sits at or above the topic's
 * {@code max.message.bytes} rather than merely above today's observed batch.
 *
 * <p>The two fetch sizes are therefore read from {@code spring.kafka.consumer.*} through
 * {@link InfraConstants.ConfigKeys}, the same per-key {@code @Value} route the group id and poll
 * size already use, so they can be retuned alongside the producer without a rebuild.
 */
@EnableKafka
@Configuration
public class KafkaConsumerConfig {

    @Value(InfraConstants.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS)
    private String bootstrapServers;

    @Value(InfraConstants.ConfigKeys.KAFKA_CONSUMER_GROUP_ID)
    private String groupId;

    @Value(InfraConstants.ConfigKeys.KAFKA_MAX_POLL_RECORDS)
    private int maxPollRecords;

    @Value(InfraConstants.ConfigKeys.KAFKA_DISPATCH_CONCURRENCY)
    private int dispatchConcurrency;

    @Value(InfraConstants.ConfigKeys.KAFKA_MAX_PARTITION_FETCH_BYTES)
    private int maxPartitionFetchBytes;

    @Value(InfraConstants.ConfigKeys.KAFKA_FETCH_MAX_BYTES)
    private int fetchMaxBytes;

    @Bean
    public ConsumerFactory<String, String> dispatchConsumerFactory() {
        Map<String, Object> config = baseConsumerConfig(groupId);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, InfraConstants.Kafka.AUTO_OFFSET_RESET_EARLIEST);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean(name = InfraConstants.Kafka.DISPATCH_LISTENER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, String> dispatchListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(dispatchConsumerFactory());

        // The offset moves only when the batch is fully resolved, which happens on another thread
        // long after the listener method returns.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Should match the partition count of the outbound topic.
        factory.setConcurrency(dispatchConcurrency);
        return factory;
    }

    /**
     * Capacity consumer.
     *
     * <p>A unique group per instance, on purpose. This is not work to be divided up — every instance
     * needs every capacity update, so each one reads the whole compacted topic independently.
     */
    @Bean
    public ConsumerFactory<String, String> capacityConsumerFactory() {
        Map<String, Object> config = baseConsumerConfig(
                groupId + InfraConstants.Kafka.CAPACITY_GROUP_SUFFIX + java.util.UUID.randomUUID());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, InfraConstants.Kafka.AUTO_OFFSET_RESET_EARLIEST);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean(name = InfraConstants.Kafka.CAPACITY_LISTENER_FACTORY)
    public ConcurrentKafkaListenerContainerFactory<String, String> capacityListenerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(capacityConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        factory.setConcurrency(InfraConstants.Kafka.CAPACITY_CONCURRENCY);
        return factory;
    }

    private Map<String, Object> baseConsumerConfig(String consumerGroup) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, InfraConstants.Kafka.FETCH_MAX_WAIT_MS);

        // Must stay at or above the outbound topic's max.message.bytes. See the class javadoc: a
        // record too large to fetch stalls its partition silently rather than failing.
        config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, maxPartitionFetchBytes);
        config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, fetchMaxBytes);
        return config;
    }
}