package com.lftairline.kafka.producer;

import com.lftairline.kafka.avro.BaggageTracking;
import com.lftairline.kafka.avro.FlightDeparture;
import com.lftairline.kafka.config.KafkaConfiguration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Kafka producer for LFT Airline events
 * Produces Avro-serialized messages to Kafka topics with Schema Registry
 */
public class FlightEventsProducer implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(FlightEventsProducer.class);
    
    private final Producer<String, Object> producer;
    private final String flightTopic;
    private final String baggageTopic;
    
    /**
     * Constructor with default configuration
     */
    public FlightEventsProducer() {
        this(KafkaConfiguration.createProducerConfig("lft-airline-producer"));
    }
    
    /**
     * Constructor with custom properties (useful for testing)
     * 
     * @param props Kafka producer properties
     */
    public FlightEventsProducer(Properties props) {
        this.producer = new KafkaProducer<>(props);
        this.flightTopic = KafkaConfiguration.FLIGHT_DEPARTURES_TOPIC;
        this.baggageTopic = KafkaConfiguration.BAGGAGE_TRACKING_TOPIC;
        logger.info("com.lftairline.kafka.producer.FlightEventsProducer initialized");
    }
    
    /**
     * Send a flight departure event asynchronously
     * 
     * @param flightDeparture the flight departure event
     * @return Future containing the record metadata
     */
    public Future<RecordMetadata> sendFlightDeparture(FlightDeparture flightDeparture) {
        String key = flightDeparture.getFlightId().toString();
        ProducerRecord<String, Object> record = 
            new ProducerRecord<>(flightTopic, key, flightDeparture);
        
        logger.debug("Sending flight departure event: flightId={}, status={}", 
                     key, flightDeparture.getStatus());
        
        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error sending flight departure event: flightId={}", 
                            key, exception);
            } else {
                logger.info("Flight departure event sent successfully: " +
                           "flightId={}, topic={}, partition={}, offset={}", 
                           key, metadata.topic(), metadata.partition(), 
                           metadata.offset());
            }
        });
    }
    
    /**
     * Send a flight departure event synchronously
     * 
     * @param flightDeparture the flight departure event
     * @return RecordMetadata containing partition and offset information
     * @throws RuntimeException if send fails
     */
    public RecordMetadata sendFlightDepartureSync(FlightDeparture flightDeparture) {
        try {
            Future<RecordMetadata> future = sendFlightDeparture(flightDeparture);
            return future.get(); // Wait for completion
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to send flight departure synchronously", e);
            throw new RuntimeException("Failed to send flight departure", e);
        }
    }
    
    /**
     * Send a baggage tracking event asynchronously
     * 
     * @param baggageTracking the baggage tracking event
     * @return Future containing the record metadata
     */
    public Future<RecordMetadata> sendBaggageTracking(BaggageTracking baggageTracking) {
        String key = baggageTracking.getBagId().toString();
        ProducerRecord<String, Object> record = 
            new ProducerRecord<>(baggageTopic, key, baggageTracking);
        
        logger.debug("Sending baggage tracking event: bagId={}, status={}", 
                     key, baggageTracking.getStatus());
        
        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error sending baggage tracking event: bagId={}", 
                            key, exception);
            } else {
                logger.info("Baggage tracking event sent successfully: " +
                           "bagId={}, topic={}, partition={}, offset={}", 
                           key, metadata.topic(), metadata.partition(), 
                           metadata.offset());
            }
        });
    }
    
    /**
     * Send a baggage tracking event synchronously
     * 
     * @param baggageTracking the baggage tracking event
     * @return RecordMetadata containing partition and offset information
     * @throws RuntimeException if send fails
     */
    public RecordMetadata sendBaggageTrackingSync(BaggageTracking baggageTracking) {
        try {
            Future<RecordMetadata> future = sendBaggageTracking(baggageTracking);
            return future.get(); // Wait for completion
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to send baggage tracking synchronously", e);
            throw new RuntimeException("Failed to send baggage tracking", e);
        }
    }
    
    /**
     * Flush any buffered records
     */
    public void flush() {
        logger.debug("Flushing producer buffers");
        producer.flush();
    }
    
    /**
     * Close the producer and release resources
     */
    @Override
    public void close() {
        logger.info("Closing com.lftairline.kafka.producer.FlightEventsProducer");
        producer.close();
    }
    
    /**
     * Main method for running the producer standalone
     */
    public static void main(String[] args) {
        logger.info("Starting LFT Airline Flight Events Producer");
        
        try (FlightEventsProducer producer = new FlightEventsProducer()) {
            // Example: Create and send flight departure events
            FlightDeparture flight1 = FlightDeparture.newBuilder()
                .setFlightId("LFT100")
                .setAircraftId("B737-800-001")
                .setDepartureAirport("JFK")
                .setArrivalAirport("LAX")
                .setScheduledDeparture(Instant.ofEpochMilli(System.currentTimeMillis() + 3600000)) // 1 hour from now
                .setActualDeparture(null)
                .setStatus(com.lftairline.kafka.avro.FlightStatus.SCHEDULED)
                .setGate("A15")
                .setPassengersBooked(180)
                .setEventTimestamp(Instant.now())
                .build();
            
            FlightDeparture flight2 = FlightDeparture.newBuilder()
                .setFlightId("LFT200")
                .setAircraftId("A320-200-002")
                .setDepartureAirport("LAX")
                .setArrivalAirport("ORD")
                .setScheduledDeparture(Instant.ofEpochMilli(System.currentTimeMillis() + 7200000)) // 2 hours from now
                .setActualDeparture(null)
                .setStatus(com.lftairline.kafka.avro.FlightStatus.BOARDING)
                .setGate("B22")
                .setPassengersBooked(150)
                .setEventTimestamp(Instant.now())
                .build();
            
            // Send flight events
            producer.sendFlightDepartureSync(flight1);
            producer.sendFlightDepartureSync(flight2);
            
            // Example: Create and send baggage tracking events
            BaggageTracking bag1 = BaggageTracking.newBuilder()
                .setBagId("BAG100001")
                .setFlightId("LFT100")
                .setPassengerId("P12345")
                .setStatus(com.lftairline.kafka.avro.BaggageStatus.CHECKED_IN)
                .setLocation("JFK")
                .setWeightKg(23.5)
                .setLastScanTime(Instant.now())
                .setEventTimestamp(Instant.now())
                .build();
            
            BaggageTracking bag2 = BaggageTracking.newBuilder()
                .setBagId("BAG100002")
                .setFlightId("LFT100")
                .setPassengerId("P12346")
                .setStatus(com.lftairline.kafka.avro.BaggageStatus.LOADED)
                .setLocation("JFK")
                .setWeightKg(18.2)
                .setLastScanTime(Instant.now())
                .setEventTimestamp(Instant.now())
                .build();
            
            // Send baggage events
            producer.sendBaggageTrackingSync(bag1);
            producer.sendBaggageTrackingSync(bag2);
            
            logger.info("All events sent successfully");
            
        } catch (Exception e) {
            logger.error("Error in producer main", e);
            System.exit(1);
        }
        
        logger.info("Producer completed successfully");
    }
}
