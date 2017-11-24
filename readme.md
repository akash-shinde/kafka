# Kafka Producer & Consumer Sample code

## Prerequisite
Before running this program, make sure your zookeeper and kafka are running. 
Follow the instructions in the link provided to bring them up:
https://kafka.apache.org/quickstart

Run the command to check if the topic "my-example-topic" exists  
``bin/kafka-topics.sh --list --zookeeper localhost:2181``

Otherwise, run the below command to create the topic kafka-topics.sh  
``--create --replication-factor 3 --partitions 5 --topic my-example-topic --zookeeper localhost:2181``

## Functionality
- The program below will start a producer thread which will send 100 messages to kafka topic.
- A message is sent every 500ms.
- Kafka consumer will run in a separate thread to consume these messages.
