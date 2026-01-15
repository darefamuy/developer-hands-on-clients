package com.lftairline.kafka.config;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Configuration class for Kafka producer and consumer settings
 * Includes Schema Registry configuration for Avro serialization
 */
public class KafkaConfiguration {
    
    // Kafka broker addresses
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    
    // Schema Registry URL
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    
    // Topic names
    public static final String FLIGHT_ARRIVALS_TOPIC = "flight-arrivals";
    public static final String PASSENGER_CHECKIN_TOPIC = "passenger-checkin";
    public static final String GATE_ASSIGNMENTS_TOPIC = "gate-assignments";
    
    /**
     * Create producer configuration with Avro serialization
     * 
     * @param clientId unique identifier for this producer
     * @return Properties object with producer configuration
     */
    public static Properties createProducerConfig(String clientId) {
        Properties props = new Properties();
        
        // Basic Kafka producer settings
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        
        // Key serializer (String)
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, 
                  StringSerializer.class.getName());
        
        // Value serializer (Avro)
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, 
                  KafkaAvroSerializer.class.getName());
        
        // Schema Registry configuration
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 
                  SCHEMA_REGISTRY_URL);
        
        // Producer reliability settings
        props.put(ProducerConfig.ACKS_CONFIG, "all"); // Wait for all replicas
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // Exactly-once
        
        // Performance tuning
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB
        
        return props;
    }
    
    /**
     * Create consumer configuration with Avro deserialization
     * 
     * @param groupId consumer group identifier
     * @param clientId unique identifier for this consumer
     * @return Properties object with consumer configuration
     */
    public static Properties createConsumerConfig(String groupId, String clientId) {
        Properties props = new Properties();
        
        // Basic Kafka consumer settings
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        
        // Key deserializer (String)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                  StringDeserializer.class.getName());
        
        // Value deserializer (Avro)
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                  KafkaAvroDeserializer.class.getName());
        
        // Schema Registry configuration
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, 
                  SCHEMA_REGISTRY_URL);
        
        // Use specific Avro reader (generated classes)
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        
        // Consumer behavior settings
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Manual commit
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000); // 5 minutes
        
        // Performance tuning
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
        
        return props;
    }
}
