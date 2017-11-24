package com.sample.kafka;

public class KafkaConsumerRunner implements Runnable {

	@Override
	public void run() {
		try {
			SimpleKafkaConsumer.runConsumer();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
