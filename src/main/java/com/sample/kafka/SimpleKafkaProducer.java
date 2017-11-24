package com.sample.kafka;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SimpleKafkaProducer {

	private static Producer<Integer, String> createProducer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", Constants.BOOTSTRAP_SERVERS);
		props.put("acks", "all");
		props.put("retries", 0);
		// Buffer size of unsent records for each partition
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

		return producer;
	}

	public static void runSynchronizedProducer(final int sendMessageCount, final long sleep) throws Exception {
		final Producer<Integer, String> producer = createProducer();
		long time = System.currentTimeMillis();
		try {
			for (int index = 0; index < sendMessageCount; index++) {
				final ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(Constants.TOPIC,
						index, "Value" + index);
				Thread.sleep(sleep);
				RecordMetadata metadata = producer.send(record).get();
				long elapsedTime = System.currentTimeMillis() - time;
				System.out.printf(">> Producer: Sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
						record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
			}
		} finally {
			producer.flush();
			producer.close();
		}
	}

	public static void runAsyncProducer(final int sendMessageCount, final long sleep) throws InterruptedException {
		final Producer<Integer, String> producer = createProducer();
		long time = System.currentTimeMillis();
		final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);
		try {
			for (int index = 0; index < sendMessageCount; index++) {
				final ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(Constants.TOPIC,
						index, "Value" + index);
				Thread.sleep(sleep);
				producer.send(record, (metadata, exception) -> {
					long elapsedTime = System.currentTimeMillis() - time;
					if (metadata != null) {
						System.out.printf(">> Producer: Sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
								record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
					} else {
						exception.printStackTrace();
					}
					countDownLatch.countDown();
				});
			}
			countDownLatch.await(25, TimeUnit.SECONDS);
		} finally {
			producer.flush();
			producer.close();
		}
	}

}
