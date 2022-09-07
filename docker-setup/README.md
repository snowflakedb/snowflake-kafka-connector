# Snowflake Sink Connector Docker 

This repository contains a sample docker compose file that can be used to start off testing [Snowflake's Kafka connector](https://docs.snowflake.com/en/user-guide/kafka-connector.html).

It has following services:

- Zookeeper
- Kafka Broker
- Schema Registry
- Kafka Connect 
- Confluent Control Center
- Creating a topic ``TEST_TOPIC`` with 4 partitions


## Install Docker and Docker Compose 

[Install Link](https://docs.docker.com/compose/install/)


### 1 - Starting the environment

Start the environment with the following command:

```bash
docker compose -f kafka-connect.yml up --remove-orphans
```

Wait until all containers are up so you can start the testing.

### 2 - Check if Control Center, Kafka, Kafka Connect is Running

You can reach confluent control-center UI with this URL
```bash
localhost:9021

```

You should see a connect cluster and depending on which connector you installed, (Snowflake Connector in this case), you should see an option to add Snowflake Sink Connector. 

You can also add a json file from UI. 

### 3 - Port exposed from Docker

Please note all docker images have its own hostname mentioned in ``kafka-connect.yml`` file

Kafka: 9092

Kafka Connect: 8083

Schema Registry: 8081

Java DEBUG Port for Connector: 5060

JMX Port for Monitoring: 9876

### 4 - Install the connector (From Command Line)

Open a terminal to execute the following command:

```bash
curl -X POST -H "Content-Type:application/json" -d @examples/sf-connector-example.json http://localhost:8083/connectors
```

### 5 - Check the data in Kafka

Open a terminal to execute the following command:
This will start the consumer on the topic

```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic source-1 --from-beginning
```

### 6.a - Generate Data: Datagen Source Connector

Use the source connector from confluent https://www.confluent.io/hub/confluentinc/kafka-connect-datagen 

Check example: ``datagen_connector.json``

### 6.b - Alternative: Use JMeter's pepper box to generate load. (In case of heavy load)

1. Install JMeter https://jmeter.apache.org/
2. Install the plugin https://github.com/GSLabDev/pepper-box
3. Send JSON records to topic Created. 

### 7 - Check Data in Snowflake Account 

Login to your account using username provided in config and verify data in the table. 

### 8 - Stop all Services

```docker compose -f kafka-connect.yml stop```

### 9 - Stop Services and remove all containers Services

Please note, this will also remove all containers, doing `docker compose up -d` will pull all containers again.

```docker compose -f kafka-connect.yml down```

# License

This project is licensed under the [Apache 2.0 License](./LICENSE).
