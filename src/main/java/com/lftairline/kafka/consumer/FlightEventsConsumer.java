package com.lftairline.kafka.consumer;

import com.lftairline.kafka.avro.BaggageTracking;
import com.lftairline.kafka.avro.FlightDeparture;
import com.lftairline.kafka.config.KafkaConfiguration;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka consumer for LFT Airline events
 * Consumes Avro-serialized messages from Kafka topics with Schema Registry
 */
public class FlightEventsConsumer implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(FlightEventsConsumer.class);
    
    private final Consumer<String, Object> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final FlightEventHandler eventHandler;
    
    /**
     * Interface for handling consumed events
     */
    public interface FlightEventHandler {
        void handleFlightDeparture(String key, FlightDeparture flightDeparture);
        void handleBaggageTracking(String key, BaggageTracking baggageTracking);
    }
    
    /**
     * Default event handler that logs events
     */
    public static class LoggingEventHandler implements FlightEventHandler {
        @Override
        public void handleFlightDeparture(String key, FlightDeparture flight) {
            logger.info("Received Flight Departure: flightId={}, status={}, " +
                       "departure={}, arrival={}, passengers={}", 
                       flight.getFlightId(), flight.getStatus(),
                       flight.getDepartureAirport(), flight.getArrivalAirport(),
                       flight.getPassengersBooked());
        }
        
        @Override
        public void handleBaggageTracking(String key, BaggageTracking baggage) {
            logger.info("Received Baggage Tracking: bagId={}, flightId={}, " +
                       "status={}, location={}, weight={}kg", 
                       baggage.getBagId(), baggage.getFlightId(),
                       baggage.getStatus(), baggage.getLocation(),
                       baggage.getWeightKg());
        }
    }
    
    /**
     * Constructor with default configuration and logging handler
     */
    public FlightEventsConsumer() {
        this(KafkaConfiguration.createConsumerConfig(
            "lft-airline-consumer-group", 
            "lft-airline-consumer-1"
        ), new LoggingEventHandler());
    }
    
    /**
     * Constructor with custom properties and event handler
     * 
     * @param props Kafka consumer properties
     * @param eventHandler handler for processing events
     */
    public FlightEventsConsumer(Properties props, FlightEventHandler eventHandler) {
        this.consumer = new KafkaConsumer<>(props);
        this.eventHandler = eventHandler;
        
        // Subscribe to topics
        consumer.subscribe(Arrays.asList(
            KafkaConfiguration.FLIGHT_DEPARTURES_TOPIC,
            KafkaConfiguration.BAGGAGE_TRACKING_TOPIC
        ));
        
        logger.info("FlightEventsConsumer initialized and subscribed to topics");
    }
    
    /**
     * Start consuming messages
     * This method blocks until shutdown() is called
     */
    public void consume() {
        logger.info("Starting to consume messages");
        
        try {
            while (running.get()) {
                ConsumerRecords<String, Object> records = 
                    consumer.poll(Duration.ofMillis(1000));
                
                if (records.isEmpty()) {
                    continue;
                }
                
                logger.debug("Polled {} records", records.count());
                
                for (ConsumerRecord<String, Object> record : records) {
                    processRecord(record);
                }
                
                // Commit offsets synchronously after processing batch
                consumer.commitSync();
                logger.debug("Committed offsets for {} records", records.count());
            }
        } catch (WakeupException e) {
            // Expected when shutting down
            logger.info("Consumer wakeup called, shutting down");
        } catch (Exception e) {
            logger.error("Error consuming messages", e);
            throw new RuntimeException("Error consuming messages", e);
        } finally {
            logger.info("Consumer shutting down");
            consumer.close();
        }
    }
    
    /**
     * Process a single record
     * 
     * @param record the consumer record to process
     */
    private void processRecord(ConsumerRecord<String, Object> record) {
        String topic = record.topic();
        String key = record.key();
        Object value = record.value();
        
        logger.debug("Processing record from topic={}, partition={}, offset={}", 
                    topic, record.partition(), record.offset());
        
        try {
            if (KafkaConfiguration.FLIGHT_DEPARTURES_TOPIC.equals(topic)) {
                if (value instanceof FlightDeparture) {
                    eventHandler.handleFlightDeparture(key, (FlightDeparture) value);
                } else {
                    logger.warn("Unexpected value type in flight-departures topic: {}", 
                               value.getClass().getName());
                }
            } else if (KafkaConfiguration.BAGGAGE_TRACKING_TOPIC.equals(topic)) {
                if (value instanceof BaggageTracking) {
                    eventHandler.handleBaggageTracking(key, (BaggageTracking) value);
                } else {
                    logger.warn("Unexpected value type in baggage-tracking topic: {}", 
                               value.getClass().getName());
                }
            } else {
                logger.warn("Received message from unexpected topic: {}", topic);
            }
        } catch (Exception e) {
            logger.error("Error processing record: topic={}, key={}", topic, key, e);
            // In production, you might want to send to DLQ (Dead Letter Queue)
        }
    }
    
    /**
     * Shutdown the consumer gracefully
     */
    public void shutdown() {
        logger.info("Shutdown requested");
        running.set(false);
        consumer.wakeup(); // Interrupt the poll() if it's blocked
    }
    
    /**
     * Close the consumer
     */
    @Override
    public void close() {
        shutdown();
    }
    
    /**
     * Main method for running the consumer standalone
     */
    public static void main(String[] args) {
        logger.info("Starting LFT Airline Flight Events Consumer");
        
        FlightEventsConsumer consumer = new FlightEventsConsumer();
        
        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered");
            consumer.shutdown();
        }));
        
        try {
            // Start consuming (blocks until shutdown)
            consumer.consume();
        } catch (Exception e) {
            logger.error("Error in consumer main", e);
            System.exit(1);
        }
        
        logger.info("Consumer completed successfully");
    }
}
