# Kafka Testing Project
This sample project is designed for testing how to use kafka with golang. It provides a simple HTTP server that exposes three endpoints for Kafka operations: creating a topic, pushing a message to a topic, and reading messages from a topic.

## Setup
Make sure you have a Kafka broker running on localhost:9092 before running the project.
``` # Clone the repository
git clone [repository_url]
cd kafka-testing-project

# Install dependencies
go get -u github.com/Shopify/sarama

# Run the project
go run main.go
```
## Endpoint
## 1. Create Kafka Topic
  - Endpoint: /create-topic
  - Method: GET
  - Parameters:
      - topic (required): Name of the topic to be created.
  - Description:
  - Creates a Kafka topic with the specified name.
## 2. Push Kafka Message

  - Endpoint: /push-message
  - Method: GET
  - Parameters:
        - topic (required): Name of the topic to which the message will be pushed.
  - Description:
  - Simulates the creation of a message and pushes it to the specified Kafka topic.
## 3. Read Kafka Message

  - Endpoint: /read-message
  - Method: GET
  - Parameters:
        - topic (required): Name of the topic from which messages will be read.
  - Description:
      - Reads messages from the specified Kafka topic. It creates a partition consumer and displays received messages.

## Usage

1. Create a Kafka topic using the /create-topic endpoint.
2. Push messages to the topic using the /push-message endpoint.
3. Read messages from the topic using the /read-message endpoint.
