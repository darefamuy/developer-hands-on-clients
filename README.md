# Hands-on Training on Kafka clients & Schema

This project contains Java-based Kafka clients that demonstrate a simple event-driven architecture using Apache Kafka. It is designed to handle events related to flight departures and baggage tracking for a fictional airline (LFT Airline). It includes a producer to send flight and baggage events, and a consumer to process these events. The communication is done using Avro-serialized messages with a Schema Registry for schema management.

# Features

- **Kafka Producer:** Sends `FlightDeparture` and `BaggageTracking` events to Kafka topics.
- **Kafka Consumer:** Subscribes to topics and processes incoming events.
- **Avro Serialization:** Uses Avro for efficient and schema-enforced data serialization.
- **Schema Registry Integration:** Manages Avro schemas centrally using the Confluent Schema Registry.
- **Gradle Build:** A comprehensive Gradle setup for building, testing, and running the applications.

## Architecture

![LFT Airline Eventstreaming](lft-airline-eventstreaming.gif)

The project consists of the following main components:

- **`AirlineEventsProducer`**: A Java application that produces records to Kafka. It can be run as a standalone process to send sample data.
- **`AirlineEventsConsumer`**: A Java application that consumes records from Kafka and logs them to the console.
- **Apache Kafka**: The message broker that decouples the producer and consumer.
- **Confluent Schema Registry**: Stores the Avro schemas used by the producer and consumer.

The following Kafka topics are used:
- `flight-arrivals`: For events related to flight status changes.
- `gate-assignment`: For events related to gate assignments for flights.
- `passenger-checkin`: For events related to passenger-checkins

## Glossary
## Data Models

The data models are defined using Avro schemas in the `src/main/avro` directory.

- **`FlightArrival.avsc`**: Represents a flight arrival event, including flight ID, airport details, status, and timestamps.
- **`GateAssignment.avsc`**: Represents a gate assignment event, including assignment ID, flight ID, gate, and timestamps.
- **`PassengerCheckin.avsc`**: Represents a passenger check-in event, including check-in ID, passenger ID, flight ID, and timestamps.

Java classes are automatically generated from these schemas during the build process.

### Flight Arrival Schema

`src/main/avro/FlightArrival.avsc`
```json
{
  "type": "record",
  "namespace": "com.lftairline.kafka.avro",
  "name": "FlightArrival",
  "doc": "Represents a flight arrival event for LFT Airline",
  "fields": [
    {
      "name": "flightId",
      "type": "string",
      "doc": "Unique flight identifier (e.g., LFT100)"
    },
    {
      "name": "aircraftId",
      "type": "string",
      "doc": "Aircraft registration number"
    },
    {
      "name": "departureAirport",
      "type": "string",
      "doc": "IATA code of departure airport (e.g., JFK)"
    },
    {
      "name": "arrivalAirport",
      "type": "string",
      "doc": "IATA code of arrival airport (e.g., LAX)"
    },
    {
      "name": "scheduledArrival",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      },
      "doc": "Scheduled arrival time in epoch milliseconds"
    },
    {
      "name": "estimatedArrival",
      "type": ["null", {
        "type": "long",
        "logicalType": "timestamp-millis"
      }],
      "default": null,
      "doc": "Estimated arrival time in epoch milliseconds (nullable)"
    },
    {
      "name": "actualArrival",
      "type": ["null", {
        "type": "long",
        "logicalType": "timestamp-millis"
      }],
      "default": null,
      "doc": "Actual arrival time in epoch milliseconds (nullable)"
    },
    {
      "name": "status",
      "type": {
        "type": "enum",
        "name": "ArrivalStatus",
        "symbols": [
          "SCHEDULED",
          "EN_ROUTE",
          "APPROACHING",
          "LANDED",
          "ARRIVED",
          "TAXIING",
          "AT_GATE",
          "DELAYED",
          "DIVERTED",
          "CANCELLED"
        ]
      },
      "doc": "Current arrival status"
    },
    {
      "name": "gate",
      "type": ["null", "string"],
      "default": null,
      "doc": "Arrival gate (nullable)"
    },
    {
      "name": "terminal",
      "type": ["null", "string"],
      "default": null,
      "doc": "Arrival terminal (nullable)"
    },
    {
      "name": "baggageClaim",
      "type": ["null", "string"],
      "default": null,
      "doc": "Baggage claim carousel number (nullable)"
    },
    {
      "name": "passengersOnBoard",
      "type": "int",
      "doc": "Number of passengers on board"
    },
    {
      "name": "eventTimestamp",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      },
      "doc": "When this event was generated"
    }
  ]
}
```

### Gate Assignment Schema

`src/main/avro/GateAssignment.avsc`
```json
{
  "type": "record",
  "namespace": "com.lftairline.kafka.avro",
  "name": "GateAssignment",
  "doc": "Represents a gate assignment event for LFT Airline",
  "fields": [
    {
      "name": "assignmentId",
      "type": "string",
      "doc": "Unique gate assignment identifier"
    },
    {
      "name": "flightId",
      "type": "string",
      "doc": "Flight identifier for this gate assignment"
    },
    {
      "name": "aircraftId",
      "type": "string",
      "doc": "Aircraft registration number"
    },
    {
      "name": "airport",
      "type": "string",
      "doc": "IATA code of the airport (e.g., JFK)"
    },
    {
      "name": "gate",
      "type": "string",
      "doc": "Gate number/identifier (e.g., A15, B22)"
    },
    {
      "name": "terminal",
      "type": "string",
      "doc": "Terminal identifier (e.g., Terminal 1, T3)"
    },
    {
      "name": "assignmentTime",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
       },
      "doc": "When the gate was assigned in epoch milliseconds"
    },
    {
      "name": "scheduledTime",
      "type": {
        "type": "long",
         "logicalType": "timestamp-millis"
      },
      "doc": "Scheduled departure/arrival time for this gate"
    },
    {
      "name": "estimatedTime",
      "type": ["null", {
        "type": "long",
        "logicalType": "timestamp-millis"
      }],
      "default": null,
      "doc": "Estimated departure/arrival time (nullable)"
    },
    {
      "name": "assignmentType",
      "type": {
        "type": "enum",
        "name": "AssignmentType",
        "symbols": [
          "DEPARTURE",
          "ARRIVAL",
          "TURNAROUND"
        ]
      },
      "doc": "Type of gate assignment (departure, arrival, or both)"
    },
    {
      "name": "previousGate",
      "type": ["null", "string"],
      "default": null,
      "doc": "Previous gate if this is a reassignment (nullable)"
    },
    {
      "name": "status",
      "type": {
        "type": "enum",
        "name": "GateStatus",
        "symbols": [
          "ASSIGNED",
          "CONFIRMED",
          "CHANGED",
          "OCCUPIED",
          "AVAILABLE",
          "MAINTENANCE",
          "CLOSED"
        ]
      },
      "doc": "Current gate status"
    },
    {
      "name": "gateEquipment",
      "type": {
        "type": "record",
        "name": "GateEquipment",
        "fields": [
          {
            "name": "jetBridge",
            "type": "boolean",
            "default": false,
            "doc": "Whether gate has a jet bridge"
          },
          {
            "name": "groundPower",
            "type": "boolean",
            "default": false,
            "doc": "Whether gate has ground power available"
          },
          {
            "name": "airConditioning",
            "type": "boolean",
            "default": false,
            "doc": "Whether gate has pre-conditioned air"
          }
        ]
      },
      "doc": "Gate equipment and capabilities"
    },
    {
      "name": "expectedPassengers",
      "type": "int",
      "doc": "Expected number of passengers for this flight"
    },
    {
      "name": "aircraftType",
      "type": ["null", "string"],
      "default": null,
      "doc": "Aircraft type (e.g., B737, A320)"
    },
    {
      "name": "notes",
      "type": ["null", "string"],
      "default": null,
      "doc": "Additional notes or instructions (nullable)"
    },
    {
      "name": "eventTimestamp",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      },
      "doc": "When this event was generated"
    }
  ]
}
```

### Passenger Check-in Schema

`src/main/avro/PassengerCheckin.avsc`
```json
{
  "type": "record",
  "namespace": "com.lftairline.kafka.avro",
  "name": "PassengerCheckin",
  "doc": "Represents a passenger check-in event for LFT Airline",
  "fields": [
    {
      "name": "checkinId",
      "type": "string",
      "doc": "Unique check-in transaction identifier"
    },
    {
      "name": "passengerId",
      "type": "string",
      "doc": "Unique passenger identifier"
    },
    {
      "name": "flightId",
      "type": "string",
      "doc": "Flight identifier for this check-in"
    },
    {
      "name": "bookingReference",
      "type": "string",
      "doc": "Booking reference/PNR code"
    },
    {
      "name": "passengerName",
      "type": {
        "type": "record",
        "name": "PassengerName",
        "fields": [
          {
            "name": "firstName",
            "type": "string",
            "doc": "Passenger first name"
          },
          {
            "name": "lastName",
            "type": "string",
            "doc": "Passenger last name"
          },
          {
            "name": "middleName",
            "type": ["null", "string"],
            "default": null,
            "doc": "Passenger middle name (optional)"
          }
        ]
      },
      "doc": "Passenger name details"
    },
    {
      "name": "checkinTime",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      },
      "doc": "Check-in timestamp in epoch milliseconds"
    },
    {
      "name": "checkinType",
      "type": {
        "type": "enum",
        "name": "CheckinType",
        "symbols": [
          "ONLINE",
          "MOBILE",
          "KIOSK",
          "COUNTER",
          "CURBSIDE"
        ]
      },
      "doc": "Type of check-in method used"
    },
    {
      "name": "seatNumber",
      "type": ["null", "string"],
      "default": null,
      "doc": "Assigned seat number (e.g., 12A)"
    },
    {
      "name": "class",
      "type": {
        "type": "enum",
        "name": "TravelClass",
        "symbols": [
          "ECONOMY",
          "PREMIUM_ECONOMY",
          "BUSINESS",
          "FIRST"
        ]
      },
      "doc": "Travel class/cabin"
    },
    {
      "name": "frequentFlyerNumber",
      "type": ["null", "string"],
      "default": null,
      "doc": "Frequent flyer membership number (optional)"
    },
    {
      "name": "baggageCount",
      "type": "int",
      "default": 0,
      "doc": "Number of checked bags"
    },
    {
      "name": "baggageIds",
      "type": {
        "type": "array",
        "items": "string"
      },
      "default": [],
      "doc": "List of baggage tag identifiers"
    },
    {
      "name": "specialRequests",
      "type": {
        "type": "array",
        "items": "string"
      },
      "default": [],
      "doc": "Special service requests (e.g., wheelchair, meal preferences)"
    },
    {
      "name": "boardingGroup",
      "type": ["null", "string"],
      "default": null,
      "doc": "Assigned boarding group (e.g., Group 1, Zone A)"
    },
    {
      "name": "status",
      "type": {
        "type": "enum",
        "name": "CheckinStatus",
        "symbols": [
          "COMPLETED",
          "PENDING_DOCS",
          "PENDING_PAYMENT",
          "CANCELLED",
          "NO_SHOW"
        ]
      },
      "doc": "Check-in status"
    },
    {
      "name": "eventTimestamp",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      },
      "doc": "When this event was generated"
    }
  ]
}
```



