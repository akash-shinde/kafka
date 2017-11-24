package com.sample.kafka;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class SimpleKafkaConsumer {

	private static Consumer<Integer, String> createConsumer() {
		final Properties props = new Properties();
		props.put("bootstrap.servers", Constants.BOOTSTRAP_SERVERS);
		// consumer group of this consumer
		props.put("group.id", "test");
		// Offsets are committed automatically with a frequency config
		// auto.commit.interval.ms
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		// Create the consumer using props.
		final Consumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(props);

		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(Constants.TOPIC));
		return consumer;
	}

	static void runConsumer() throws InterruptedException {
		final Consumer<Integer, String> consumer = createConsumer();

		final int giveUp = 100;
		int noRecordsCount = 0;

		while (true) {
			final ConsumerRecords<Integer, String> consumerRecords = consumer.poll(1000);

			if (consumerRecords.count() == 0) {
				noRecordsCount++;
				if (noRecordsCount > giveUp)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.printf("<< Consumer: Record:(key=%d, value=%s, partition=%d, offset=%d)\n", record.key(), record.value(),
						record.partition(), record.offset());
			});

			consumer.commitAsync();
		}
		consumer.close();
		System.out.println("DONE");
	}

}
