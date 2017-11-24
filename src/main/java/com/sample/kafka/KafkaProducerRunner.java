package com.sample.kafka;

public class KafkaProducerRunner implements Runnable {

	private int sendMessageCount;
	private String type;
	private long sleep;

	public KafkaProducerRunner(int sendMessageCount, String type, long sleep) {
		this.type = type;
		this.sendMessageCount = sendMessageCount;
		this.sleep = sleep;
	}

	@Override
	public void run() {
		try {
			if (type == "sync") {
				SimpleKafkaProducer.runSynchronizedProducer(sendMessageCount, sleep);
			} else if (type == "async") {
				SimpleKafkaProducer.runAsyncProducer(sendMessageCount, sleep);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
