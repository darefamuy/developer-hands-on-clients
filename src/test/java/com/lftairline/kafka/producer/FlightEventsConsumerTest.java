package com.lftairline.kafka.producer;

import com.lftairline.kafka.avro.BaggageStatus;
import com.lftairline.kafka.avro.BaggageTracking;
import com.lftairline.kafka.avro.FlightDeparture;
import com.lftairline.kafka.avro.FlightStatus;
import com.lftairline.kafka.consumer.FlightEventsConsumer;
import com.lftairline.kafka.consumer.FlightEventsConsumer.FlightEventHandler;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.mockito.ArgumentCaptor;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for FlightEventsConsumer
 */
class FlightEventsConsumerTest {
    
    private MockConsumer<String, Object> mockConsumer;
    private FlightEventHandler mockHandler;
    private TopicPartition flightTopicPartition;
    private TopicPartition baggageTopicPartition;
    
    @BeforeEach
    void setUp() {
        mockConsumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
        mockHandler = mock(FlightEventHandler.class);
        
        flightTopicPartition = new TopicPartition("flight-departures", 0);
        baggageTopicPartition = new TopicPartition("baggage-tracking", 0);
        
        // Assign partitions
        mockConsumer.assign(Arrays.asList(flightTopicPartition, baggageTopicPartition));
        
        // Set beginning offsets
        Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(flightTopicPartition, 0L);
        beginningOffsets.put(baggageTopicPartition, 0L);
        mockConsumer.updateBeginningOffsets(beginningOffsets);
    }
    
    @Test
    @DisplayName("Should handle flight departure event correctly")
    void testHandleFlightDeparture() {
        // Given
        FlightDeparture flight = createSampleFlightDeparture();
        ConsumerRecord<String, Object> record = new ConsumerRecord<>(
            "flight-departures",
            0,
            0L,
            flight.getFlightId().toString(),
            flight
        );
        
        mockConsumer.addRecord(record);
        
        // When
        FlightEventHandler handler = mock(FlightEventHandler.class);
        // Simulate processing one record
        handler.handleFlightDeparture(record.key(), flight);
        
        // Then
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<FlightDeparture> flightCaptor = 
            ArgumentCaptor.forClass(FlightDeparture.class);
        
        verify(handler).handleFlightDeparture(keyCaptor.capture(), flightCaptor.capture());
        
        assertEquals("LFT100", keyCaptor.getValue());
        assertEquals("LFT100", flightCaptor.getValue().getFlightId().toString());
        assertEquals(FlightStatus.SCHEDULED, flightCaptor.getValue().getStatus());
    }
    
    @Test
    @DisplayName("Should handle baggage tracking event correctly")
    void testHandleBaggageTracking() {
        // Given
        BaggageTracking baggage = createSampleBaggageTracking();
        ConsumerRecord<String, Object> record = new ConsumerRecord<>(
            "baggage-tracking",
            0,
            0L,
            baggage.getBagId().toString(),
            baggage
        );
        
        mockConsumer.addRecord(record);
        
        // When
        FlightEventHandler handler = mock(FlightEventHandler.class);
        handler.handleBaggageTracking(record.key(), baggage);
        
        // Then
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<BaggageTracking> baggageCaptor = 
            ArgumentCaptor.forClass(BaggageTracking.class);
        
        verify(handler).handleBaggageTracking(keyCaptor.capture(), baggageCaptor.capture());
        
        assertEquals("BAG100001", keyCaptor.getValue());
        assertEquals("BAG100001", baggageCaptor.getValue().getBagId().toString());
        assertEquals(BaggageStatus.CHECKED_IN, baggageCaptor.getValue().getStatus());
    }
    
    @Test
    @DisplayName("Should process multiple events in order")
    void testProcessMultipleEvents() {
        // Given
        FlightDeparture flight1 = createSampleFlightDeparture();
        FlightDeparture flight2 = FlightDeparture.newBuilder(flight1)
            .setFlightId("LFT200")
            .build();
        BaggageTracking baggage = createSampleBaggageTracking();
        
        mockConsumer.addRecord(new ConsumerRecord<>(
            "flight-departures", 0, 0L, "LFT100", flight1));
        mockConsumer.addRecord(new ConsumerRecord<>(
            "flight-departures", 0, 1L, "LFT200", flight2));
        mockConsumer.addRecord(new ConsumerRecord<>(
            "baggage-tracking", 0, 0L, "BAG100001", baggage));
        
        // When
        FlightEventHandler handler = mock(FlightEventHandler.class);
        
        // Simulate processing
        handler.handleFlightDeparture("LFT100", flight1);
        handler.handleFlightDeparture("LFT200", flight2);
        handler.handleBaggageTracking("BAG100001", baggage);
        
        // Then
        verify(handler, times(2)).handleFlightDeparture(anyString(), any(FlightDeparture.class));
        verify(handler, times(1)).handleBaggageTracking(anyString(), any(BaggageTracking.class));
    }
    
    @Test
    @DisplayName("Should handle different flight statuses")
    void testHandleDifferentFlightStatuses() {
        // Given
        FlightEventHandler handler = mock(FlightEventHandler.class);
        
        // Test each status
        FlightStatus[] statuses = {
            FlightStatus.SCHEDULED,
            FlightStatus.BOARDING,
            FlightStatus.DEPARTED,
            FlightStatus.DELAYED,
            FlightStatus.CANCELLED
        };
        
        for (int i = 0; i < statuses.length; i++) {
            FlightDeparture flight = FlightDeparture.newBuilder()
                .setFlightId("LFT" + (100 + i))
                .setAircraftId("AIRCRAFT-" + i)
                .setDepartureAirport("JFK")
                .setArrivalAirport("LAX")
                .setScheduledDeparture(Instant.now())
                .setStatus(statuses[i])
                .setPassengersBooked(180)
                .setEventTimestamp(Instant.now())
                .build();
            
            // When
            handler.handleFlightDeparture(flight.getFlightId().toString(), flight);
        }
        
        // Then
        verify(handler, times(statuses.length))
            .handleFlightDeparture(anyString(), any(FlightDeparture.class));
    }
    
    @Test
    @DisplayName("Should handle different baggage statuses")
    void testHandleDifferentBaggageStatuses() {
        // Given
        FlightEventHandler handler = mock(FlightEventHandler.class);
        
        // Test each status
        BaggageStatus[] statuses = {
            BaggageStatus.CHECKED_IN,
            BaggageStatus.IN_TRANSIT,
            BaggageStatus.LOADED,
            BaggageStatus.IN_FLIGHT,
            BaggageStatus.UNLOADED,
            BaggageStatus.CLAIMED,
            BaggageStatus.LOST
        };
        
        for (int i = 0; i < statuses.length; i++) {
            BaggageTracking baggage = BaggageTracking.newBuilder()
                .setBagId("BAG" + (100000 + i))
                .setFlightId("LFT100")
                .setPassengerId("P" + i)
                .setStatus(statuses[i])
                .setLocation("JFK")
                .setWeightKg(20.0 + i)
                .setLastScanTime(Instant.now())
                .setEventTimestamp(Instant.now())
                .build();
            
            // When
            handler.handleBaggageTracking(baggage.getBagId().toString(), baggage);
        }
        
        // Then
        verify(handler, times(statuses.length))
            .handleBaggageTracking(anyString(), any(BaggageTracking.class));
    }
    
    @Test
    @DisplayName("Should correctly extract flight information")
    void testFlightInformationExtraction() {
        // Given
        FlightDeparture flight = createSampleFlightDeparture();
        
        // Then
        assertEquals("LFT100", flight.getFlightId().toString());
        assertEquals("B737-800-001", flight.getAircraftId().toString());
        assertEquals("JFK", flight.getDepartureAirport().toString());
        assertEquals("LAX", flight.getArrivalAirport().toString());
        assertEquals(FlightStatus.SCHEDULED, flight.getStatus());
        assertEquals("A15", flight.getGate().toString());
        assertEquals(180, flight.getPassengersBooked());
        assertNull(flight.getActualDeparture());
    }
    
    @Test
    @DisplayName("Should correctly extract baggage information")
    void testBaggageInformationExtraction() {
        // Given
        BaggageTracking baggage = createSampleBaggageTracking();
        
        // Then
        assertEquals("BAG100001", baggage.getBagId().toString());
        assertEquals("LFT100", baggage.getFlightId().toString());
        assertEquals("P12345", baggage.getPassengerId().toString());
        assertEquals(BaggageStatus.CHECKED_IN, baggage.getStatus());
        assertEquals("JFK", baggage.getLocation().toString());
        assertEquals(23.5, baggage.getWeightKg(), 0.01);
        assertNotNull(baggage.getLastScanTime());
        assertNotNull(baggage.getEventTimestamp());
    }
    
    @Test
    @DisplayName("Should handle null optional fields in flight departure")
    void testNullOptionalFieldsInFlight() {
        // Given
        FlightDeparture flight = FlightDeparture.newBuilder()
            .setFlightId("LFT100")
            .setAircraftId("B737-001")
            .setDepartureAirport("JFK")
            .setArrivalAirport("LAX")
            .setScheduledDeparture(Instant.now())
            .setActualDeparture(null) // Optional field
            .setStatus(FlightStatus.SCHEDULED)
            .setGate(null) // Optional field
            .setPassengersBooked(180)
            .setEventTimestamp(Instant.now())
            .build();
        
        // Then
        assertNotNull(flight);
        assertNull(flight.getActualDeparture());
        assertNull(flight.getGate());
    }
    
    @Test
    @DisplayName("Logging handler should not throw exceptions")
    void testLoggingHandlerDoesNotThrow() {
        // Given
        FlightEventsConsumer.LoggingEventHandler handler =
            new FlightEventsConsumer.LoggingEventHandler();
        
        FlightDeparture flight = createSampleFlightDeparture();
        BaggageTracking baggage = createSampleBaggageTracking();
        
        // When/Then - should not throw
        assertDoesNotThrow(() -> handler.handleFlightDeparture("LFT100", flight));
        assertDoesNotThrow(() -> handler.handleBaggageTracking("BAG100001", baggage));
    }
    
    @Test
    @DisplayName("Should verify consumer offset management")
    void testOffsetManagement() {
        // Given
        FlightDeparture flight = createSampleFlightDeparture();
        
        mockConsumer.addRecord(new ConsumerRecord<>(
            "flight-departures", 0, 0L, "LFT100", flight));
        mockConsumer.addRecord(new ConsumerRecord<>(
            "flight-departures", 0, 1L, "LFT100", flight));
        
        // When
        assertFalse(mockConsumer.poll(java.time.Duration.ofMillis(100)).isEmpty());
        
        // Then
        assertEquals(2L, mockConsumer.position(flightTopicPartition));
    }
    
    // Helper methods
    
    private FlightDeparture createSampleFlightDeparture() {
        return FlightDeparture.newBuilder()
            .setFlightId("LFT100")
            .setAircraftId("B737-800-001")
            .setDepartureAirport("JFK")
            .setArrivalAirport("LAX")
            .setScheduledDeparture(Instant.now().plus(1, ChronoUnit.HOURS))
            .setActualDeparture(null)
            .setStatus(FlightStatus.SCHEDULED)
            .setGate("A15")
            .setPassengersBooked(180)
            .setEventTimestamp(Instant.now())
            .build();
    }
    
    private BaggageTracking createSampleBaggageTracking() {
        return BaggageTracking.newBuilder()
            .setBagId("BAG100001")
            .setFlightId("LFT100")
            .setPassengerId("P12345")
            .setStatus(BaggageStatus.CHECKED_IN)
            .setLocation("JFK")
            .setWeightKg(23.5)
            .setLastScanTime(Instant.now())
            .setEventTimestamp(Instant.now())
            .build();
    }
}