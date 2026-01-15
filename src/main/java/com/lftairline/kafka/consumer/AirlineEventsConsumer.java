package com.lftairline.kafka.consumer;

import com.lftairline.kafka.avro.*;
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
 * Extended Kafka consumer for all LFT Airline event types
 * Consumes Avro-serialized messages for arrivals, check-ins, and gate assignments
 */
public class AirlineEventsConsumer implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(AirlineEventsConsumer.class);
    
    private final Consumer<String, Object> consumer;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AirlineEventHandler eventHandler;
    
    /**
     * Interface for handling consumed airline events
     */
    public interface AirlineEventHandler {
        void handleFlightArrival(String key, FlightArrival flightArrival);
        void handlePassengerCheckin(String key, PassengerCheckin passengerCheckin);
        void handleGateAssignment(String key, GateAssignment gateAssignment);
    }
    
    /**
     * Default event handler that logs events
     */
    public static class LoggingEventHandler implements AirlineEventHandler {
        @Override
        public void handleFlightArrival(String key, FlightArrival arrival) {
            logger.info("Received Flight Arrival: flightId={}, status={}, " +
                       "arrival={}, gate={}, passengers={}", 
                       arrival.getFlightId(), arrival.getStatus(),
                       arrival.getArrivalAirport(), arrival.getGate(),
                       arrival.getPassengersOnBoard());
        }
        
        @Override
        public void handlePassengerCheckin(String key, PassengerCheckin checkin) {
            logger.info("Received Passenger Check-in: checkinId={}, passenger={} {}, " +
                       "flightId={}, seat={}, class={}", 
                       checkin.getCheckinId(), 
                       checkin.getPassengerName().getFirstName(),
                       checkin.getPassengerName().getLastName(),
                       checkin.getFlightId(), checkin.getSeatNumber(),
                       checkin.getClass$());
        }
        
        @Override
        public void handleGateAssignment(String key, GateAssignment assignment) {
            logger.info("Received Gate Assignment: assignmentId={}, flightId={}, " +
                       "gate={}, terminal={}, status={}", 
                       assignment.getAssignmentId(), assignment.getFlightId(),
                       assignment.getGate(), assignment.getTerminal(),
                       assignment.getStatus());
        }
    }
    
    /**
     * Constructor with default configuration and logging handler
     */
    public AirlineEventsConsumer() {
        this(KafkaConfiguration.createConsumerConfig(
            "lft-airline-events-consumer-group", 
            "lft-airline-events-consumer-1"
        ), new LoggingEventHandler());
    }
    
    /**
     * Constructor with custom properties and event handler
     * 
     * @param props Kafka consumer properties
     * @param eventHandler handler for processing events
     */
    public AirlineEventsConsumer(Properties props, AirlineEventHandler eventHandler) {
        this.consumer = new KafkaConsumer<>(props);
        this.eventHandler = eventHandler;
        
        // Subscribe to all airline event topics
        consumer.subscribe(Arrays.asList(
            KafkaConfiguration.FLIGHT_ARRIVALS_TOPIC,
            KafkaConfiguration.PASSENGER_CHECKIN_TOPIC,
            KafkaConfiguration.GATE_ASSIGNMENTS_TOPIC
        ));
        
        logger.info("AirlineEventsConsumer initialized and subscribed to topics");
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
            if (KafkaConfiguration.FLIGHT_ARRIVALS_TOPIC.equals(topic)) {
                if (value instanceof FlightArrival) {
                    eventHandler.handleFlightArrival(key, (FlightArrival) value);
                } else {
                    logger.warn("Unexpected value type in flight-arrivals topic: {}", 
                               value.getClass().getName());
                }
            } else if (KafkaConfiguration.PASSENGER_CHECKIN_TOPIC.equals(topic)) {
                if (value instanceof PassengerCheckin) {
                    eventHandler.handlePassengerCheckin(key, (PassengerCheckin) value);
                } else {
                    logger.warn("Unexpected value type in passenger-checkin topic: {}", 
                               value.getClass().getName());
                }
            } else if (KafkaConfiguration.GATE_ASSIGNMENTS_TOPIC.equals(topic)) {
                if (value instanceof GateAssignment) {
                    eventHandler.handleGateAssignment(key, (GateAssignment) value);
                } else {
                    logger.warn("Unexpected value type in gate-assignments topic: {}", 
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
        logger.info("Starting LFT Airline Events Consumer");
        
        AirlineEventsConsumer consumer = new AirlineEventsConsumer();
        
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
