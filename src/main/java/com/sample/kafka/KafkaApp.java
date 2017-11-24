package com.sample.kafka;

public class KafkaApp {

	/*
	 * Before running this program, make sure your zookeeper and kafka are
	 * running. Follow the instructions in the link provided to bring them up:
	 * https://kafka.apache.org/quickstart
	 * 
	 * Run the command to check if the topic "my-example-topic" exists
	 * bin/kafka-topics.sh --list --zookeeper localhost:2181
	 * 
	 * Otherwise, run the below command to create the topic bin/kafka-topics.sh
	 * --create --replication-factor 3 --partitions 5 --topic my-example-topic --zookeeper localhost:2181
	 * 
	 * The program below will start a producer thread which will send 100
	 * messages to kafka topic.
	 * Kafka consumer will run in a separate thread to consume these messages
	 * 
	 */

	public static void main(String[] args) throws Exception {

		int numMessages = 100;
		String mode = "sync";
		long sleep = 500l;
		
		// Run a producer thread
		KafkaProducerRunner producer = new KafkaProducerRunner(numMessages, mode, sleep);
		Thread producerThread = new Thread(producer);
		producerThread.start();

		// Run a consumer thread
		KafkaConsumerRunner consumer = new KafkaConsumerRunner();
		Thread consumerThread = new Thread(consumer);
		consumerThread.start();
	}

}
