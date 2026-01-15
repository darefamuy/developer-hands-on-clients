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

- **`FlightEventsProducer`**: A Java application that produces records to Kafka. It can be run as a standalone process to send sample data.
- **`FlightEventsConsumer`**: A Java application that consumes records from Kafka and logs them to the console.
- **Confluent Kafka**: The message broker that decouples the producer and consumer.
- **Confluent Schema Registry**: Stores the Avro schemas used by the producer and consumer.

The following Kafka topics are used:
- `flight-departures`: For events related to flight status changes.
- `baggage-tracking`: For events related to baggage status changes.

## Data Models

The data models are defined using Avro schemas in the `src/main/avro` directory.

- **`FlightDeparture.avsc`**: Represents a flight departure event, including flight ID, airport details, status, and timestamps.
- **`BaggageTracking.avsc`**: Represents a baggage tracking event, including bag ID, flight ID, status, and location.

Java classes are automatically generated from these schemas during the build process.

## Prerequisites

Before you begin, ensure you have the following installed:

- **Java 11** or later

Additionally, you will need access to a running Apache Kafka cluster and Confluent Schema Registry. The client applications are configured to connect to `localhost:9092` for Kafka brokers and `http://localhost:8081` for the Schema Registry by default.

## Getting Started

Follow these steps to get the project up and running on your local machine.

### 1. Clone the Repository

```bash
git clone <repository-url>
cd developer-hands-on-clients
```

### 2. Build the Project

Use the included Gradle wrapper to build the project. This will also trigger the `generateAvro` task to compile the Avro schemas into Java source files.

```bash
./gradlew build
```

You can also manually generate the Avro classes if needed:

```bash
./gradlew generateAvro
```

## Usage

The producer and consumer applications can be run using custom Gradle tasks. It's recommended to run them in separate terminal windows to observe the interaction.

### Run the Consumer

Start the consumer first to ensure it's ready to receive messages.

```bash
./gradlew runConsumer
```

The consumer will start, subscribe to the topics, and begin polling for messages. You will see log output indicating it has started.

### Run the Producer

In a new terminal, run the producer. It will send a predefined set of sample `FlightDeparture` and `BaggageTracking` events and then exit.

```bash
./gradlew runProducer
```

After the producer runs, you should see log output in the consumer's terminal window showing the messages it has received and processed.

## Configuration

The Kafka client configuration is centralized in the `com.lftairline.kafka.config.KafkaConfiguration` class.

- **Kafka Brokers**: `BOOTSTRAP_SERVERS` is currently set to `localhost:9092`. 
- **Schema Registry**: `SCHEMA_REGISTRY_URL` is currently set to `http://localhost:8081`.
- **Topics**: Topic names (`flight-departures`, `baggage-tracking`) are also defined here.

If you may need to connect to a  Kafka cluster, you can modify the constants in this file.
Please change both the Kafka bootstrapServer endpoint and schema registry URL to the ones provided for this training.

## Running Tests

To run the unit and integration tests for the project, execute the following command:

```bash
./gradlew test
```

## Running in Docker

To spin up a local Kafka cluster in KRaft mode with a local schema registry, execute the command below.
Once the services are up and running, you can then re-configure the application with appropriate local addresses and re-run it.

```bash
docker-compose up -d
```

## Glossary

### Flight Departure Schema

src/main/avro/FlightDeparture.avsc
```json
{
  "type": "record",
  "namespace": "com.lftairline.kafka.avro",
  "name": "FlightDeparture",
  "doc": "Represents a flight departure event for LFT Airline",
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
      "name": "scheduledDeparture",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      },
      "doc": "Scheduled departure time in epoch milliseconds"
    },
    {
      "name": "actualDeparture",
      "type": ["null", {
        "type": "long",
        "logicalType": "timestamp-millis"
      }],
      "default": null,
      "doc": "Actual departure time in epoch milliseconds (nullable)"
    },
    {
      "name": "status",
      "type": {
        "type": "enum",
        "name": "FlightStatus",
        "symbols": [
          "SCHEDULED",
          "BOARDING",
          "DEPARTED",
          "IN_FLIGHT",
          "ARRIVED",
          "DELAYED",
          "CANCELLED"
        ]
      },
      "doc": "Current flight status"
    },
    {
      "name": "gate",
      "type": ["null", "string"],
      "default": null,
      "doc": "Departure gate (nullable)"
    },
    {
      "name": "passengersBooked",
      "type": "int",
      "doc": "Number of passengers booked on this flight"
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

### Baggage Tracking Schema

src/main/avro/BaggageTracking.avsc
```json
{
  "type": "record",
  "namespace": "com.lftairline.kafka.avro",
  "name": "BaggageTracking",
  "doc": "Represents a baggage tracking event for LFT Airline",
  "fields": [
    {
      "name": "bagId",
      "type": "string",
      "doc": "Unique baggage identifier"
    },
    {
      "name": "flightId",
      "type": "string",
      "doc": "Associated flight identifier"
    },
    {
      "name": "passengerId",
      "type": "string",
      "doc": "Passenger identifier who owns this bag"
    },
    {
      "name": "status",
      "type": {
        "type": "enum",
        "name": "BaggageStatus",
        "symbols": [
          "CHECKED_IN",
          "IN_TRANSIT",
          "LOADED",
          "IN_FLIGHT",
          "UNLOADED",
          "CLAIMED",
          "LOST"
        ]
      },
      "doc": "Current baggage status"
    },
    {
      "name": "location",
      "type": "string",
      "doc": "Current location (airport code or facility)"
    },
    {
      "name": "weightKg",
      "type": "double",
      "doc": "Weight of baggage in kilograms"
    },
    {
      "name": "lastScanTime",
      "type": {
        "type": "long",
        "logicalType": "timestamp-millis"
      },
      "doc": "Last scan timestamp in epoch milliseconds"
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



