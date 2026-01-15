package com.lftairline.kafka.producer;

import com.lftairline.kafka.avro.*;
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
 * Extended Kafka producer for all LFT Airline event types
 * Produces Avro-serialized messages for arrivals, check-ins, and gate assignments
 */
public class AirlineEventsProducer implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(AirlineEventsProducer.class);
    
    private final Producer<String, Object> producer;
    private final String arrivalsTopicName;
    private final String checkinTopicName;
    private final String gateAssignmentsTopicName;
    
    /**
     * Constructor with default configuration
     */
    public AirlineEventsProducer() {
        this(KafkaConfiguration.createProducerConfig("lft-airline-events-producer"));
    }
    
    /**
     * Constructor with custom properties (useful for testing)
     * 
     * @param props Kafka producer properties
     */
    public AirlineEventsProducer(Properties props) {
        this.producer = new KafkaProducer<>(props);
        this.arrivalsTopicName = KafkaConfiguration.FLIGHT_ARRIVALS_TOPIC;
        this.checkinTopicName = KafkaConfiguration.PASSENGER_CHECKIN_TOPIC;
        this.gateAssignmentsTopicName = KafkaConfiguration.GATE_ASSIGNMENTS_TOPIC;
        logger.info("AirlineEventsProducer initialized");
    }
    
    /**
     * Send a flight arrival event asynchronously
     * 
     * @param flightArrival the flight arrival event
     * @return Future containing the record metadata
     */
    public Future<RecordMetadata> sendFlightArrival(FlightArrival flightArrival) {
        String key = flightArrival.getFlightId().toString();
        ProducerRecord<String, Object> record = 
            new ProducerRecord<>(arrivalsTopicName, key, flightArrival);
        
        logger.debug("Sending flight arrival event: flightId={}, status={}", 
                     key, flightArrival.getStatus());
        
        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error sending flight arrival event: flightId={}", 
                            key, exception);
            } else {
                logger.info("Flight arrival event sent successfully: " +
                           "flightId={}, topic={}, partition={}, offset={}", 
                           key, metadata.topic(), metadata.partition(), 
                           metadata.offset());
            }
        });
    }
    
    /**
     * Send a flight arrival event synchronously
     * 
     * @param flightArrival the flight arrival event
     * @return RecordMetadata containing partition and offset information
     * @throws RuntimeException if send fails
     */
    public RecordMetadata sendFlightArrivalSync(FlightArrival flightArrival) {
        try {
            Future<RecordMetadata> future = sendFlightArrival(flightArrival);
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to send flight arrival synchronously", e);
            throw new RuntimeException("Failed to send flight arrival", e);
        }
    }
    
    /**
     * Send a passenger check-in event asynchronously
     * 
     * @param passengerCheckin the passenger check-in event
     * @return Future containing the record metadata
     */
    public Future<RecordMetadata> sendPassengerCheckin(PassengerCheckin passengerCheckin) {
        String key = passengerCheckin.getCheckinId().toString();
        ProducerRecord<String, Object> record = 
            new ProducerRecord<>(checkinTopicName, key, passengerCheckin);
        
        logger.debug("Sending passenger check-in event: checkinId={}, passengerId={}", 
                     key, passengerCheckin.getPassengerId());
        
        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error sending passenger check-in event: checkinId={}", 
                            key, exception);
            } else {
                logger.info("Passenger check-in event sent successfully: " +
                           "checkinId={}, topic={}, partition={}, offset={}", 
                           key, metadata.topic(), metadata.partition(), 
                           metadata.offset());
            }
        });
    }
    
    /**
     * Send a passenger check-in event synchronously
     * 
     * @param passengerCheckin the passenger check-in event
     * @return RecordMetadata containing partition and offset information
     * @throws RuntimeException if send fails
     */
    public RecordMetadata sendPassengerCheckinSync(PassengerCheckin passengerCheckin) {
        try {
            Future<RecordMetadata> future = sendPassengerCheckin(passengerCheckin);
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to send passenger check-in synchronously", e);
            throw new RuntimeException("Failed to send passenger check-in", e);
        }
    }
    
    /**
     * Send a gate assignment event asynchronously
     * 
     * @param gateAssignment the gate assignment event
     * @return Future containing the record metadata
     */
    public Future<RecordMetadata> sendGateAssignment(GateAssignment gateAssignment) {
        String key = gateAssignment.getAssignmentId().toString();
        ProducerRecord<String, Object> record = 
            new ProducerRecord<>(gateAssignmentsTopicName, key, gateAssignment);
        
        logger.debug("Sending gate assignment event: assignmentId={}, gate={}", 
                     key, gateAssignment.getGate());
        
        return producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error sending gate assignment event: assignmentId={}", 
                            key, exception);
            } else {
                logger.info("Gate assignment event sent successfully: " +
                           "assignmentId={}, topic={}, partition={}, offset={}", 
                           key, metadata.topic(), metadata.partition(), 
                           metadata.offset());
            }
        });
    }
    
    /**
     * Send a gate assignment event synchronously
     * 
     * @param gateAssignment the gate assignment event
     * @return RecordMetadata containing partition and offset information
     * @throws RuntimeException if send fails
     */
    public RecordMetadata sendGateAssignmentSync(GateAssignment gateAssignment) {
        try {
            Future<RecordMetadata> future = sendGateAssignment(gateAssignment);
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Failed to send gate assignment synchronously", e);
            throw new RuntimeException("Failed to send gate assignment", e);
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
        logger.info("Closing AirlineEventsProducer");
        producer.close();
    }
    
    /**
     * Main method for running the producer standalone
     */
    public static void main(String[] args) {
        logger.info("Starting LFT Airline Events Producer");
        
        try (AirlineEventsProducer producer = new AirlineEventsProducer()) {
            
            // Example 1: Flight Arrival Event
            FlightArrival arrival = FlightArrival.newBuilder()
                .setFlightId("LFT300")
                .setAircraftId("A320-200-003")
                .setDepartureAirport("LAX")
                .setArrivalAirport("SFO")
                .setScheduledArrival(Instant.ofEpochMilli(System.currentTimeMillis() + 1800000)) // 30 min
                .setEstimatedArrival(Instant.ofEpochMilli(System.currentTimeMillis() + 1500000)) // 25 min
                .setActualArrival(null)
                .setStatus(ArrivalStatus.EN_ROUTE)
                .setGate("C10")
                .setTerminal("Terminal 2")
                .setBaggageClaim("Carousel 5")
                .setPassengersOnBoard(150)
                .setEventTimestamp(Instant.ofEpochMilli(System.currentTimeMillis()))
                .build();
            
            producer.sendFlightArrivalSync(arrival);
            logger.info("Flight arrival event sent: {}", arrival.getFlightId());
            
            // Example 2: Passenger Check-in Event
            PassengerName passengerName = PassengerName.newBuilder()
                .setFirstName("John")
                .setLastName("Smith")
                .setMiddleName("Michael")
                .build();
            
            PassengerCheckin checkin = PassengerCheckin.newBuilder()
                .setCheckinId("CHK100001")
                .setPassengerId("P12345")
                .setFlightId("LFT300")
                .setBookingReference("ABC123")
                .setPassengerName(passengerName)
                .setCheckinTime(Instant.ofEpochMilli(System.currentTimeMillis()))
                .setCheckinType(CheckinType.MOBILE)
                .setSeatNumber("12A")
                .setClass$(TravelClass.ECONOMY)
                .setFrequentFlyerNumber("FF987654")
                .setBaggageCount(2)
                .setBaggageIds(java.util.Arrays.asList("BAG200001", "BAG200002"))
                .setSpecialRequests(java.util.Arrays.asList("Vegetarian meal"))
                .setBoardingGroup("Group 2")
                .setStatus(CheckinStatus.COMPLETED)
                .setEventTimestamp(Instant.ofEpochMilli(System.currentTimeMillis()))
                .build();
            
            producer.sendPassengerCheckinSync(checkin);
            logger.info("Passenger check-in event sent: {}", checkin.getCheckinId());
            
            // Example 3: Gate Assignment Event
            GateEquipment equipment = GateEquipment.newBuilder()
                .setJetBridge(true)
                .setGroundPower(true)
                .setAirConditioning(true)
                .build();
            
            GateAssignment assignment = GateAssignment.newBuilder()
                .setAssignmentId("GA100001")
                .setFlightId("LFT300")
                .setAircraftId("A320-200-003")
                .setAirport("SFO")
                .setGate("C10")
                .setTerminal("Terminal 2")
                .setAssignmentTime(Instant.ofEpochMilli(System.currentTimeMillis()))
                .setScheduledTime(Instant.ofEpochMilli(System.currentTimeMillis() + 1800000))
                .setEstimatedTime(Instant.ofEpochMilli(System.currentTimeMillis() + 1500000))
                .setAssignmentType(AssignmentType.ARRIVAL)
                .setPreviousGate(null)
                .setStatus(GateStatus.CONFIRMED)
                .setGateEquipment(equipment)
                .setExpectedPassengers(150)
                .setAircraftType("A320")
                .setNotes("Normal arrival")
                .setEventTimestamp(Instant.ofEpochMilli(System.currentTimeMillis()))
                .build();
            
            producer.sendGateAssignmentSync(assignment);
            logger.info("Gate assignment event sent: {}", assignment.getAssignmentId());
            
            logger.info("All events sent successfully");
            
        } catch (Exception e) {
            logger.error("Error in producer main", e);
            System.exit(1);
        }
        
        logger.info("Producer completed successfully");
    }
}
