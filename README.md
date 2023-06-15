# Real-Time-Vehicle-Stream-Processing-with-Apache-Flink-and-Kafka
This application is designed to process real-time stream data of vehicles using Apache Flink and Kafka. It retrieves data from a Kafka topic and processes it in real-time, enabling batch inserts based on a defined time difference. The processed data is then stored in an HBase using GeoMesa.

## Features
- Fetches real-time data streams from a Kafka topic.
- Processes the incoming Kafka data in real-time.
- Stores the processed data in an HBase using GeoMesa.
- Two separate stores for live and history data.
- Error handling for empty or missing data.

## Requirements
- Java 8 or higher
- Apache Kafka
- Apache Flink
- Apache HBase
- GeoMesa

## How to run
Update the "bootstrapServers" variable with your Kafka server address and run the DiakLC class.

## Code Overview
This application is centered around the DiakLC class, which contains the main method. Here is an outline of what the application does:

- Sets up the Kafka Consumer and creates a datastream from this source.
- Processes the Kafka messages, including handling for any empty messages.
- Connects to two separate HBase DataStores (diak:lc_live and diak:lc_history) using GeoMesa.
- Defines a SimpleFeatureType for each store and creates the respective schema.
- For each incoming message from Kafka, it constructs a SimpleFeature for both stores.
- Writes the constructed features to the respective store.
- At the end of the job, it closes the DataStores and prints out processing stats.
- The application can handle a large number of real-time data streams efficiently and offers error handling to ensure that processing does not stop due to issues with a single data point.

## Configuration
You can adjust the Kafka and HBase settings in the FlinkKafkaConsumerVehicles Java class to suit your needs.

### Kafka Configuration:

- Kafka Bootstrap servers: "your kafka boostrap servers address"
- Kafka Group ID: "diak_vehicles"
- Kafka Topic: "diak_vehicles"

### HBase Configuration:

- HBase tables: "diak:vehicles_live" and "diak:vehicles_history"
- Zookeeper addresses are hardcoded as "ts-dlr-bs,ts-ts-dlr-bs" for both live and history tables

## Note
- Ensure that HBase and Kafka services are running before executing the application.
- Please update the Zookeeper addresses to your own configurations.
- The Kafka topic needs to be pre-created and the vehicle data should be published to it.
- Flink job is set to start from the beginning of the Kafka topic. Please change this setting if needed.

## Contribute
Contributions are welcome! Please feel free to submit a Pull Request.
