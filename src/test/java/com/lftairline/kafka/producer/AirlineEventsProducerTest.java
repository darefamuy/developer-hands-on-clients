package com.lftairline.kafka.producer;

import com.lftairline.kafka.avro.*;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for AirlineEventsProducer
 */
class AirlineEventsProducerTest {
    
    private MockProducer<String, Object> mockProducer;
    
    @BeforeEach
    void setUp() {
        mockProducer = new MockProducer<>(
            true, // auto-complete sends
            new StringSerializer(),
            new org.apache.kafka.common.serialization.Serializer<Object>() {
                @Override
                public byte[] serialize(String topic, Object data) {
                    return new byte[0];
                }
            }
        );
    }
    
    @Test
    @DisplayName("Should send flight arrival event successfully")
    void testSendFlightArrival() {
        // Given
        FlightArrival arrival = createSampleFlightArrival();
        
        // When
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "flight-arrivals",
            arrival.getFlightId().toString(),
            arrival
        );
        mockProducer.send(record);
        
        // Then
        List<ProducerRecord<String, Object>> history = mockProducer.history();
        assertEquals(1, history.size());
        
        ProducerRecord<String, Object> sentRecord = history.get(0);
        assertEquals("flight-arrivals", sentRecord.topic());
        assertEquals("LFT300", sentRecord.key());
        assertTrue(sentRecord.value() instanceof FlightArrival);
        
        FlightArrival sentArrival = (FlightArrival) sentRecord.value();
        assertEquals("LFT300", sentArrival.getFlightId().toString());
        assertEquals("SFO", sentArrival.getArrivalAirport().toString());
        assertEquals(ArrivalStatus.EN_ROUTE, sentArrival.getStatus());
    }
    
    @Test
    @DisplayName("Should send passenger check-in event successfully")
    void testSendPassengerCheckin() {
        // Given
        PassengerCheckin checkin = createSamplePassengerCheckin();
        
        // When
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "passenger-checkin",
            checkin.getCheckinId().toString(),
            checkin
        );
        mockProducer.send(record);
        
        // Then
        List<ProducerRecord<String, Object>> history = mockProducer.history();
        assertEquals(1, history.size());
        
        ProducerRecord<String, Object> sentRecord = history.get(0);
        assertEquals("passenger-checkin", sentRecord.topic());
        assertEquals("CHK100001", sentRecord.key());
        assertTrue(sentRecord.value() instanceof PassengerCheckin);
        
        PassengerCheckin sentCheckin = (PassengerCheckin) sentRecord.value();
        assertEquals("CHK100001", sentCheckin.getCheckinId().toString());
        assertEquals("P12345", sentCheckin.getPassengerId().toString());
        assertEquals(CheckinStatus.COMPLETED, sentCheckin.getStatus());
    }
    
    @Test
    @DisplayName("Should send gate assignment event successfully")
    void testSendGateAssignment() {
        // Given
        GateAssignment assignment = createSampleGateAssignment();
        
        // When
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "gate-assignments",
            assignment.getAssignmentId().toString(),
            assignment
        );
        mockProducer.send(record);
        
        // Then
        List<ProducerRecord<String, Object>> history = mockProducer.history();
        assertEquals(1, history.size());
        
        ProducerRecord<String, Object> sentRecord = history.get(0);
        assertEquals("gate-assignments", sentRecord.topic());
        assertEquals("GA100001", sentRecord.key());
        assertTrue(sentRecord.value() instanceof GateAssignment);
        
        GateAssignment sentAssignment = (GateAssignment) sentRecord.value();
        assertEquals("GA100001", sentAssignment.getAssignmentId().toString());
        assertEquals("C10", sentAssignment.getGate().toString());
        assertEquals(GateStatus.CONFIRMED, sentAssignment.getStatus());
    }
    
    @Test
    @DisplayName("Should send multiple events in sequence")
    void testSendMultipleEvents() {
        // Given
        FlightArrival arrival = createSampleFlightArrival();
        PassengerCheckin checkin = createSamplePassengerCheckin();
        GateAssignment assignment = createSampleGateAssignment();
        
        // When
        mockProducer.send(new ProducerRecord<>("flight-arrivals", 
            arrival.getFlightId().toString(), arrival));
        mockProducer.send(new ProducerRecord<>("passenger-checkin", 
            checkin.getCheckinId().toString(), checkin));
        mockProducer.send(new ProducerRecord<>("gate-assignments", 
            assignment.getAssignmentId().toString(), assignment));
        
        // Then
        List<ProducerRecord<String, Object>> history = mockProducer.history();
        assertEquals(3, history.size());
        
        assertEquals("LFT300", history.get(0).key());
        assertEquals("CHK100001", history.get(1).key());
        assertEquals("GA100001", history.get(2).key());
    }
    
    @Test
    @DisplayName("Should handle all arrival statuses")
    void testAllArrivalStatuses() {
        ArrivalStatus[] statuses = ArrivalStatus.values();
        
        assertEquals(10, statuses.length);
        assertTrue(Arrays.asList(statuses).contains(ArrivalStatus.SCHEDULED));
        assertTrue(Arrays.asList(statuses).contains(ArrivalStatus.EN_ROUTE));
        assertTrue(Arrays.asList(statuses).contains(ArrivalStatus.LANDED));
        assertTrue(Arrays.asList(statuses).contains(ArrivalStatus.AT_GATE));
    }
    
    @Test
    @DisplayName("Should handle all check-in types")
    void testAllCheckinTypes() {
        CheckinType[] types = CheckinType.values();
        
        assertEquals(5, types.length);
        assertTrue(Arrays.asList(types).contains(CheckinType.ONLINE));
        assertTrue(Arrays.asList(types).contains(CheckinType.MOBILE));
        assertTrue(Arrays.asList(types).contains(CheckinType.KIOSK));
        assertTrue(Arrays.asList(types).contains(CheckinType.COUNTER));
    }
    
    @Test
    @DisplayName("Should handle all travel classes")
    void testAllTravelClasses() {
        TravelClass[] classes = TravelClass.values();
        
        assertEquals(4, classes.length);
        assertTrue(Arrays.asList(classes).contains(TravelClass.ECONOMY));
        assertTrue(Arrays.asList(classes).contains(TravelClass.BUSINESS));
        assertTrue(Arrays.asList(classes).contains(TravelClass.FIRST));
    }
    
    @Test
    @DisplayName("Should handle all gate statuses")
    void testAllGateStatuses() {
        GateStatus[] statuses = GateStatus.values();
        
        assertEquals(7, statuses.length);
        assertTrue(Arrays.asList(statuses).contains(GateStatus.ASSIGNED));
        assertTrue(Arrays.asList(statuses).contains(GateStatus.CONFIRMED));
        assertTrue(Arrays.asList(statuses).contains(GateStatus.OCCUPIED));
    }
    
    @Test
    @DisplayName("Should validate passenger name structure")
    void testPassengerNameStructure() {
        PassengerName name = PassengerName.newBuilder()
            .setFirstName("John")
            .setLastName("Smith")
            .setMiddleName("Michael")
            .build();
        
        assertEquals("John", name.getFirstName().toString());
        assertEquals("Smith", name.getLastName().toString());
        assertEquals("Michael", name.getMiddleName().toString());
    }
    
    @Test
    @DisplayName("Should validate gate equipment structure")
    void testGateEquipmentStructure() {
        GateEquipment equipment = GateEquipment.newBuilder()
            .setJetBridge(true)
            .setGroundPower(true)
            .setAirConditioning(false)
            .build();
        
        assertTrue(equipment.getJetBridge());
        assertTrue(equipment.getGroundPower());
        assertFalse(equipment.getAirConditioning());
    }
    
    @Test
    @DisplayName("Should handle passenger check-in with baggage")
    void testPassengerCheckinWithBaggage() {
        PassengerCheckin checkin = createSamplePassengerCheckin();
        
        assertEquals(2, checkin.getBaggageCount());
        assertEquals(2, checkin.getBaggageIds().size());
        assertTrue(checkin.getBaggageIds().contains("BAG200001"));
        assertTrue(checkin.getBaggageIds().contains("BAG200002"));
    }
    
    @Test
    @DisplayName("Should handle gate assignment type")
    void testGateAssignmentType() {
        GateAssignment assignment = createSampleGateAssignment();
        
        assertEquals(AssignmentType.ARRIVAL, assignment.getAssignmentType());
        
        AssignmentType[] types = AssignmentType.values();
        assertEquals(3, types.length);
        assertTrue(Arrays.asList(types).contains(AssignmentType.DEPARTURE));
        assertTrue(Arrays.asList(types).contains(AssignmentType.ARRIVAL));
        assertTrue(Arrays.asList(types).contains(AssignmentType.TURNAROUND));
    }
    
    @Test
    @DisplayName("Should use correct partition key for arrival events")
    void testArrivalPartitionKey() {
        FlightArrival arrival = createSampleFlightArrival();
        String expectedKey = arrival.getFlightId().toString();
        
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "flight-arrivals",
            expectedKey,
            arrival
        );
        mockProducer.send(record);
        
        ProducerRecord<String, Object> sentRecord = mockProducer.history().get(0);
        assertEquals(expectedKey, sentRecord.key());
    }
    
    @Test
    @DisplayName("Should use correct partition key for checkin events")
    void testCheckinPartitionKey() {
        PassengerCheckin checkin = createSamplePassengerCheckin();
        String expectedKey = checkin.getCheckinId().toString();
        
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "passenger-checkin",
            expectedKey,
            checkin
        );
        mockProducer.send(record);
        
        ProducerRecord<String, Object> sentRecord = mockProducer.history().get(0);
        assertEquals(expectedKey, sentRecord.key());
    }
    
    @Test
    @DisplayName("Should use correct partition key for gate assignment events")
    void testGateAssignmentPartitionKey() {
        GateAssignment assignment = createSampleGateAssignment();
        String expectedKey = assignment.getAssignmentId().toString();
        
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "gate-assignments",
            expectedKey,
            assignment
        );
        mockProducer.send(record);
        
        ProducerRecord<String, Object> sentRecord = mockProducer.history().get(0);
        assertEquals(expectedKey, sentRecord.key());
    }
    
    // Helper methods
    
    private FlightArrival createSampleFlightArrival() {
        return FlightArrival.newBuilder()
            .setFlightId("LFT300")
            .setAircraftId("A320-200-003")
            .setDepartureAirport("LAX")
            .setArrivalAirport("SFO")
            .setScheduledArrival(Instant.ofEpochMilli(System.currentTimeMillis() + 1800000))
            .setEstimatedArrival(Instant.ofEpochMilli(System.currentTimeMillis() + 1500000))
            .setActualArrival(null)
            .setStatus(ArrivalStatus.EN_ROUTE)
            .setGate("C10")
            .setTerminal("Terminal 2")
            .setBaggageClaim("Carousel 5")
            .setPassengersOnBoard(150)
            .setEventTimestamp(Instant.now())
            .build();
    }
    
    private PassengerCheckin createSamplePassengerCheckin() {
        PassengerName name = PassengerName.newBuilder()
            .setFirstName("John")
            .setLastName("Smith")
            .setMiddleName("Michael")
            .build();
        
        return PassengerCheckin.newBuilder()
            .setCheckinId("CHK100001")
            .setPassengerId("P12345")
            .setFlightId("LFT300")
            .setBookingReference("ABC123")
            .setPassengerName(name)
            .setCheckinTime(Instant.now())
            .setCheckinType(CheckinType.MOBILE)
            .setSeatNumber("12A")
            .setClass$(TravelClass.ECONOMY)
            .setFrequentFlyerNumber("FF987654")
            .setBaggageCount(2)
            .setBaggageIds(Arrays.asList("BAG200001", "BAG200002"))
            .setSpecialRequests(Arrays.asList("Vegetarian meal"))
            .setBoardingGroup("Group 2")
            .setStatus(CheckinStatus.COMPLETED)
            .setEventTimestamp(Instant.now())
            .build();
    }
    
    private GateAssignment createSampleGateAssignment() {
        GateEquipment equipment = GateEquipment.newBuilder()
            .setJetBridge(true)
            .setGroundPower(true)
            .setAirConditioning(true)
            .build();
        
        return GateAssignment.newBuilder()
            .setAssignmentId("GA100001")
            .setFlightId("LFT300")
            .setAircraftId("A320-200-003")
            .setAirport("SFO")
            .setGate("C10")
            .setTerminal("Terminal 2")
            .setAssignmentTime(Instant.now())
            .setScheduledTime(Instant.ofEpochMilli(System.currentTimeMillis() + 1800000))
            .setEstimatedTime(Instant.ofEpochMilli(System.currentTimeMillis() + 1500000))
            .setAssignmentType(AssignmentType.ARRIVAL)
            .setPreviousGate(null)
            .setStatus(GateStatus.CONFIRMED)
            .setGateEquipment(equipment)
            .setExpectedPassengers(150)
            .setAircraftType("A320")
            .setNotes("Normal arrival")
            .setEventTimestamp(Instant.now())
            .build();
    }
}
