package com.lftairline.kafka.producer;

import com.lftairline.kafka.avro.BaggageStatus;
import com.lftairline.kafka.avro.BaggageTracking;
import com.lftairline.kafka.avro.FlightDeparture;
import com.lftairline.kafka.avro.FlightStatus;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for FlightEventsProducer
 */
class FlightEventsProducerTest {
    
    private MockProducer<String, Object> mockProducer;
    
    @BeforeEach
    void setUp() {
        // Create mock producer with auto-complete enabled
        mockProducer = new MockProducer<>(
            true, // auto-complete sends
            new StringSerializer(),
            new org.apache.kafka.common.serialization.Serializer<Object>() {
                @Override
                public byte[] serialize(String topic, Object data) {
                    return new byte[0]; // Mock serialization
                }
            }
        );
    }
    
    @Test
    @DisplayName("Should send flight departure event successfully")
    void testSendFlightDeparture() {
        // Given
        FlightDeparture flight = createSampleFlightDeparture();
        
        // When
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "flight-departures",
            flight.getFlightId().toString(),
            flight
        );
        mockProducer.send(record);
        
        // Then
        List<ProducerRecord<String, Object>> history = mockProducer.history();
        assertEquals(1, history.size());
        
        ProducerRecord<String, Object> sentRecord = history.get(0);
        assertEquals("flight-departures", sentRecord.topic());
        assertEquals("LFT100", sentRecord.key());
        assertTrue(sentRecord.value() instanceof FlightDeparture);
        
        FlightDeparture sentFlight = (FlightDeparture) sentRecord.value();
        assertEquals("LFT100", sentFlight.getFlightId().toString());
        assertEquals("JFK", sentFlight.getDepartureAirport().toString());
        assertEquals("LAX", sentFlight.getArrivalAirport().toString());
        assertEquals(FlightStatus.SCHEDULED, sentFlight.getStatus());
    }
    
    @Test
    @DisplayName("Should send baggage tracking event successfully")
    void testSendBaggageTracking() {
        // Given
        BaggageTracking baggage = createSampleBaggageTracking();
        
        // When
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "baggage-tracking",
            baggage.getBagId().toString(),
            baggage
        );
        mockProducer.send(record);
        
        // Then
        List<ProducerRecord<String, Object>> history = mockProducer.history();
        assertEquals(1, history.size());
        
        ProducerRecord<String, Object> sentRecord = history.get(0);
        assertEquals("baggage-tracking", sentRecord.topic());
        assertEquals("BAG100001", sentRecord.key());
        assertTrue(sentRecord.value() instanceof BaggageTracking);
        
        BaggageTracking sentBaggage = (BaggageTracking) sentRecord.value();
        assertEquals("BAG100001", sentBaggage.getBagId().toString());
        assertEquals("LFT100", sentBaggage.getFlightId().toString());
        assertEquals(BaggageStatus.CHECKED_IN, sentBaggage.getStatus());
    }
    
    @Test
    @DisplayName("Should send multiple events in sequence")
    void testSendMultipleEvents() {
        // Given
        FlightDeparture flight1 = createSampleFlightDeparture();
        FlightDeparture flight2 = FlightDeparture.newBuilder(flight1)
            .setFlightId("LFT200")
            .setStatus(FlightStatus.BOARDING)
            .build();
        
        BaggageTracking bag1 = createSampleBaggageTracking();
        
        // When
        mockProducer.send(new ProducerRecord<>("flight-departures", 
            flight1.getFlightId().toString(), flight1));
        mockProducer.send(new ProducerRecord<>("flight-departures", 
            flight2.getFlightId().toString(), flight2));
        mockProducer.send(new ProducerRecord<>("baggage-tracking", 
            bag1.getBagId().toString(), bag1));
        
        // Then
        List<ProducerRecord<String, Object>> history = mockProducer.history();
        assertEquals(3, history.size());
        
        // Verify order
        assertEquals("LFT100", history.get(0).key());
        assertEquals("LFT200", history.get(1).key());
        assertEquals("BAG100001", history.get(2).key());
    }
    
    @Test
    @DisplayName("Should handle producer errors gracefully")
    void testProducerError() {
        // Given
        MockProducer<String, Object> errorProducer = new MockProducer<>(
            false, // manual completion
            new StringSerializer(),
            new org.apache.kafka.common.serialization.Serializer<Object>() {
                @Override
                public byte[] serialize(String topic, Object data) {
                    return new byte[0];
                }
            }
        );
        
        FlightDeparture flight = createSampleFlightDeparture();
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "flight-departures",
            flight.getFlightId().toString(),
            flight
        );
        
        // When
        Future<RecordMetadata> future = errorProducer.send(record);
        
        // Simulate error
        RuntimeException exception = new RuntimeException("Network error");
        errorProducer.errorNext(exception);
        
        // Then
        assertTrue(errorProducer.history().size() > 0);
        assertTrue(future.isDone());
        
        assertThrows(Exception.class, () -> future.get());
    }
    
    @Test
    @DisplayName("Should validate flight departure required fields")
    void testFlightDepartureValidation() {
        // When/Then - Building without required fields should work with defaults
        FlightDeparture flight = FlightDeparture.newBuilder()
            .setFlightId("LFT100")
            .setAircraftId("B737-001")
            .setDepartureAirport("JFK")
            .setArrivalAirport("LAX")
            .setScheduledDeparture(Instant.now())
            .setStatus(FlightStatus.SCHEDULED)
            .setPassengersBooked(180)
            .setEventTimestamp(Instant.now())
            .build();
        
        assertNotNull(flight);
        assertEquals("LFT100", flight.getFlightId().toString());
        assertNull(flight.getActualDeparture());
        assertNull(flight.getGate());
    }
    
    @Test
    @DisplayName("Should validate baggage tracking required fields")
    void testBaggageTrackingValidation() {
        // When/Then
        BaggageTracking baggage = BaggageTracking.newBuilder()
            .setBagId("BAG001")
            .setFlightId("LFT100")
            .setPassengerId("P001")
            .setStatus(BaggageStatus.CHECKED_IN)
            .setLocation("JFK")
            .setWeightKg(23.5)
            .setLastScanTime(Instant.now())
            .setEventTimestamp(Instant.now())
            .build();
        
        assertNotNull(baggage);
        assertEquals("BAG001", baggage.getBagId().toString());
        assertEquals(23.5, baggage.getWeightKg(), 0.01);
    }
    
    @Test
    @DisplayName("Should correctly set all flight statuses")
    void testFlightStatuses() {
        FlightDeparture flight = createSampleFlightDeparture();
        
        // Test all possible statuses
        FlightStatus[] statuses = FlightStatus.values();
        for (FlightStatus status : statuses) {
            FlightDeparture updatedFlight = FlightDeparture.newBuilder(flight)
                .setStatus(status)
                .build();
            
            assertEquals(status, updatedFlight.getStatus());
        }
    }
    
    @Test
    @DisplayName("Should correctly set all baggage statuses")
    void testBaggageStatuses() {
        BaggageTracking baggage = createSampleBaggageTracking();
        
        // Test all possible statuses
        BaggageStatus[] statuses = BaggageStatus.values();
        for (BaggageStatus status : statuses) {
            BaggageTracking updatedBaggage = BaggageTracking.newBuilder(baggage)
                .setStatus(status)
                .build();
            
            assertEquals(status, updatedBaggage.getStatus());
        }
    }
    
    @Test
    @DisplayName("Should use correct partition key for flight events")
    void testFlightPartitionKey() {
        // Given
        FlightDeparture flight = createSampleFlightDeparture();
        String expectedKey = flight.getFlightId().toString();
        
        // When
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "flight-departures",
            expectedKey,
            flight
        );
        mockProducer.send(record);
        
        // Then
        ProducerRecord<String, Object> sentRecord = mockProducer.history().get(0);
        assertEquals(expectedKey, sentRecord.key());
    }
    
    @Test
    @DisplayName("Should use correct partition key for baggage events")
    void testBaggagePartitionKey() {
        // Given
        BaggageTracking baggage = createSampleBaggageTracking();
        String expectedKey = baggage.getBagId().toString();
        
        // When
        ProducerRecord<String, Object> record = new ProducerRecord<>(
            "baggage-tracking",
            expectedKey,
            baggage
        );
        mockProducer.send(record);
        
        // Then
        ProducerRecord<String, Object> sentRecord = mockProducer.history().get(0);
        assertEquals(expectedKey, sentRecord.key());
    }
    
    // Helper methods
    
    private FlightDeparture createSampleFlightDeparture() {
        return FlightDeparture.newBuilder()
            .setFlightId("LFT100")
            .setAircraftId("B737-800-001")
            .setDepartureAirport("JFK")
            .setArrivalAirport("LAX")
            .setScheduledDeparture(Instant.ofEpochMilli(System.currentTimeMillis() + 3600000))
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