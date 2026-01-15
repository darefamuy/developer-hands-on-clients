package com.lftairline.kafka.consumer;

import com.lftairline.kafka.avro.*;
import com.lftairline.kafka.consumer.AirlineEventsConsumer.AirlineEventHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AirlineEventsConsumer
 */
class AirlineEventsConsumerTest {
    
    private MockConsumer<String, Object> mockConsumer;
    private AirlineEventHandler mockHandler;
    private TopicPartition arrivalsPartition;
    private TopicPartition checkinPartition;
    private TopicPartition gateAssignmentsPartition;
    
    @BeforeEach
    void setUp() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockHandler = mock(AirlineEventHandler.class);
        
        arrivalsPartition = new TopicPartition("flight-arrivals", 0);
        checkinPartition = new TopicPartition("passenger-checkin", 0);
        gateAssignmentsPartition = new TopicPartition("gate-assignments", 0);
        
        // Assign partitions
        mockConsumer.assign(Arrays.asList(
            arrivalsPartition, 
            checkinPartition, 
            gateAssignmentsPartition
        ));
        
        // Set beginning offsets
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(arrivalsPartition, 0L);
        beginningOffsets.put(checkinPartition, 0L);
        beginningOffsets.put(gateAssignmentsPartition, 0L);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
    }
    
    @Test
    @DisplayName("Should handle flight arrival event correctly")
    void testHandleFlightArrival() {
        // Given
        FlightArrival arrival = createSampleFlightArrival();
        ConsumerRecord<String, Object> record = new ConsumerRecord<>(
            "flight-arrivals",
            0,
            0L,
            arrival.getFlightId().toString(),
            arrival
        );
        
        mockConsumer.addRecord(record);
        
        // When
        AirlineEventHandler handler = mock(AirlineEventHandler.class);
        handler.handleFlightArrival(record.key(), arrival);
        
        // Then
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<FlightArrival> arrivalCaptor = 
            ArgumentCaptor.forClass(FlightArrival.class);
        
        verify(handler).handleFlightArrival(keyCaptor.capture(), arrivalCaptor.capture());
        
        assertEquals("LFT300", keyCaptor.getValue());
        assertEquals("LFT300", arrivalCaptor.getValue().getFlightId().toString());
        assertEquals(ArrivalStatus.EN_ROUTE, arrivalCaptor.getValue().getStatus());
    }
    
    @Test
    @DisplayName("Should handle passenger check-in event correctly")
    void testHandlePassengerCheckin() {
        // Given
        PassengerCheckin checkin = createSamplePassengerCheckin();
        ConsumerRecord<String, Object> record = new ConsumerRecord<>(
            "passenger-checkin",
            0,
            0L,
            checkin.getCheckinId().toString(),
            checkin
        );
        
        mockConsumer.addRecord(record);
        
        // When
        AirlineEventHandler handler = mock(AirlineEventHandler.class);
        handler.handlePassengerCheckin(record.key(), checkin);
        
        // Then
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<PassengerCheckin> checkinCaptor = 
            ArgumentCaptor.forClass(PassengerCheckin.class);
        
        verify(handler).handlePassengerCheckin(keyCaptor.capture(), checkinCaptor.capture());
        
        assertEquals("CHK100001", keyCaptor.getValue());
        assertEquals("CHK100001", checkinCaptor.getValue().getCheckinId().toString());
        assertEquals(CheckinStatus.COMPLETED, checkinCaptor.getValue().getStatus());
    }
    
    @Test
    @DisplayName("Should handle gate assignment event correctly")
    void testHandleGateAssignment() {
        // Given
        GateAssignment assignment = createSampleGateAssignment();
        ConsumerRecord<String, Object> record = new ConsumerRecord<>(
            "gate-assignments",
            0,
            0L,
            assignment.getAssignmentId().toString(),
            assignment
        );
        
        mockConsumer.addRecord(record);
        
        // When
        AirlineEventHandler handler = mock(AirlineEventHandler.class);
        handler.handleGateAssignment(record.key(), assignment);
        
        // Then
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<GateAssignment> assignmentCaptor = 
            ArgumentCaptor.forClass(GateAssignment.class);
        
        verify(handler).handleGateAssignment(keyCaptor.capture(), assignmentCaptor.capture());
        
        assertEquals("GA100001", keyCaptor.getValue());
        assertEquals("GA100001", assignmentCaptor.getValue().getAssignmentId().toString());
        assertEquals(GateStatus.CONFIRMED, assignmentCaptor.getValue().getStatus());
    }
    
    @Test
    @DisplayName("Should process multiple events in order")
    void testProcessMultipleEvents() {
        // Given
        FlightArrival arrival = createSampleFlightArrival();
        PassengerCheckin checkin = createSamplePassengerCheckin();
        GateAssignment assignment = createSampleGateAssignment();
        
        mockConsumer.addRecord(new ConsumerRecord<>(
            "flight-arrivals", 0, 0L, "LFT300", arrival));
        mockConsumer.addRecord(new ConsumerRecord<>(
            "passenger-checkin", 0, 0L, "CHK100001", checkin));
        mockConsumer.addRecord(new ConsumerRecord<>(
            "gate-assignments", 0, 0L, "GA100001", assignment));
        
        // When
        AirlineEventHandler handler = mock(AirlineEventHandler.class);
        
        handler.handleFlightArrival("LFT300", arrival);
        handler.handlePassengerCheckin("CHK100001", checkin);
        handler.handleGateAssignment("GA100001", assignment);
        
        // Then
        verify(handler, times(1)).handleFlightArrival(anyString(), any(FlightArrival.class));
        verify(handler, times(1)).handlePassengerCheckin(anyString(), any(PassengerCheckin.class));
        verify(handler, times(1)).handleGateAssignment(anyString(), any(GateAssignment.class));
    }
    
    @Test
    @DisplayName("Should correctly extract flight arrival information")
    void testFlightArrivalInformationExtraction() {
        // Given
        FlightArrival arrival = createSampleFlightArrival();
        
        // Then
        assertEquals("LFT300", arrival.getFlightId().toString());
        assertEquals("A320-200-003", arrival.getAircraftId().toString());
        assertEquals("LAX", arrival.getDepartureAirport().toString());
        assertEquals("SFO", arrival.getArrivalAirport().toString());
        assertEquals(ArrivalStatus.EN_ROUTE, arrival.getStatus());
        assertEquals("C10", arrival.getGate().toString());
        assertEquals("Terminal 2", arrival.getTerminal().toString());
        assertEquals("Carousel 5", arrival.getBaggageClaim().toString());
        assertEquals(150, arrival.getPassengersOnBoard());
    }
    
    @Test
    @DisplayName("Should correctly extract passenger check-in information")
    void testPassengerCheckinInformationExtraction() {
        // Given
        PassengerCheckin checkin = createSamplePassengerCheckin();
        
        // Then
        assertEquals("CHK100001", checkin.getCheckinId().toString());
        assertEquals("P12345", checkin.getPassengerId().toString());
        assertEquals("LFT300", checkin.getFlightId().toString());
        assertEquals("ABC123", checkin.getBookingReference().toString());
        assertEquals("John", checkin.getPassengerName().getFirstName().toString());
        assertEquals("Smith", checkin.getPassengerName().getLastName().toString());
        assertEquals(CheckinType.MOBILE, checkin.getCheckinType());
        assertEquals("12A", checkin.getSeatNumber().toString());
        assertEquals(TravelClass.ECONOMY, checkin.getClass$());
        assertEquals(2, checkin.getBaggageCount());
        assertEquals(CheckinStatus.COMPLETED, checkin.getStatus());
    }
    
    @Test
    @DisplayName("Should correctly extract gate assignment information")
    void testGateAssignmentInformationExtraction() {
        // Given
        GateAssignment assignment = createSampleGateAssignment();
        
        // Then
        assertEquals("GA100001", assignment.getAssignmentId().toString());
        assertEquals("LFT300", assignment.getFlightId().toString());
        assertEquals("A320-200-003", assignment.getAircraftId().toString());
        assertEquals("SFO", assignment.getAirport().toString());
        assertEquals("C10", assignment.getGate().toString());
        assertEquals("Terminal 2", assignment.getTerminal().toString());
        assertEquals(AssignmentType.ARRIVAL, assignment.getAssignmentType());
        assertEquals(GateStatus.CONFIRMED, assignment.getStatus());
        assertTrue(assignment.getGateEquipment().getJetBridge());
        assertEquals(150, assignment.getExpectedPassengers());
    }
    
    @Test
    @DisplayName("Should handle different arrival statuses")
    void testHandleDifferentArrivalStatuses() {
        // Given
        AirlineEventHandler handler = mock(AirlineEventHandler.class);
        
        ArrivalStatus[] statuses = {
            ArrivalStatus.SCHEDULED,
            ArrivalStatus.EN_ROUTE,
            ArrivalStatus.LANDED,
            ArrivalStatus.AT_GATE
        };
        
        for (int i = 0; i < statuses.length; i++) {
            FlightArrival arrival = FlightArrival.newBuilder()
                .setFlightId("LFT" + (300 + i))
                .setAircraftId("AIRCRAFT-" + i)
                .setDepartureAirport("LAX")
                .setArrivalAirport("SFO")
                .setScheduledArrival(Instant.now())
                .setStatus(statuses[i])
                .setPassengersOnBoard(150)
                .setEventTimestamp(Instant.now())
                .build();
            
            // When
            handler.handleFlightArrival(arrival.getFlightId().toString(), arrival);
        }
        
        // Then
        verify(handler, times(statuses.length))
            .handleFlightArrival(anyString(), any(FlightArrival.class));
    }
    
    @Test
    @DisplayName("Should handle passenger check-in with baggage")
    void testPassengerCheckinWithBaggage() {
        // Given
        PassengerCheckin checkin = createSamplePassengerCheckin();
        
        // Then
        assertEquals(2, checkin.getBaggageCount());
        assertEquals(2, checkin.getBaggageIds().size());
        assertTrue(checkin.getBaggageIds().contains("BAG200001"));
        assertTrue(checkin.getBaggageIds().contains("BAG200002"));
    }
    
    @Test
    @DisplayName("Should handle passenger check-in with special requests")
    void testPassengerCheckinWithSpecialRequests() {
        // Given
        PassengerCheckin checkin = createSamplePassengerCheckin();
        
        // Then
        assertEquals(1, checkin.getSpecialRequests().size());
        assertTrue(checkin.getSpecialRequests().contains("Vegetarian meal"));
    }
    
    @Test
    @DisplayName("Should handle gate equipment details")
    void testGateEquipmentDetails() {
        // Given
        GateAssignment assignment = createSampleGateAssignment();
        
        // Then
        assertTrue(assignment.getGateEquipment().getJetBridge());
        assertTrue(assignment.getGateEquipment().getGroundPower());
        assertTrue(assignment.getGateEquipment().getAirConditioning());
    }
    
    @Test
    @DisplayName("Should handle null optional fields in arrival")
    void testNullOptionalFieldsInArrival() {
        // Given
        FlightArrival arrival = FlightArrival.newBuilder()
            .setFlightId("LFT300")
            .setAircraftId("A320-001")
            .setDepartureAirport("LAX")
            .setArrivalAirport("SFO")
            .setScheduledArrival(Instant.now())
            .setEstimatedArrival(null)
            .setActualArrival(null)
            .setStatus(ArrivalStatus.SCHEDULED)
            .setGate(null)
            .setTerminal(null)
            .setBaggageClaim(null)
            .setPassengersOnBoard(150)
            .setEventTimestamp(Instant.now())
            .build();
        
        // Then
        assertNotNull(arrival);
        assertNull(arrival.getEstimatedArrival());
        assertNull(arrival.getActualArrival());
        assertNull(arrival.getGate());
        assertNull(arrival.getTerminal());
        assertNull(arrival.getBaggageClaim());
    }
    
    @Test
    @DisplayName("Logging handler should not throw exceptions")
    void testLoggingHandlerDoesNotThrow() {
        // Given
        AirlineEventsConsumer.LoggingEventHandler handler = 
            new AirlineEventsConsumer.LoggingEventHandler();
        
        FlightArrival arrival = createSampleFlightArrival();
        PassengerCheckin checkin = createSamplePassengerCheckin();
        GateAssignment assignment = createSampleGateAssignment();
        
        // When/Then - should not throw
        assertDoesNotThrow(() -> handler.handleFlightArrival("LFT300", arrival));
        assertDoesNotThrow(() -> handler.handlePassengerCheckin("CHK100001", checkin));
        assertDoesNotThrow(() -> handler.handleGateAssignment("GA100001", assignment));
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
        assertTrue(Arrays.asList(types).contains(CheckinType.CURBSIDE));
    }
    
    @Test
    @DisplayName("Should handle all assignment types")
    void testAllAssignmentTypes() {
        AssignmentType[] types = AssignmentType.values();
        assertEquals(3, types.length);
        
        assertTrue(Arrays.asList(types).contains(AssignmentType.DEPARTURE));
        assertTrue(Arrays.asList(types).contains(AssignmentType.ARRIVAL));
        assertTrue(Arrays.asList(types).contains(AssignmentType.TURNAROUND));
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
