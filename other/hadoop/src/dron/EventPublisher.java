package org.apache.hadoop.mapred.dron;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class EventPublisher implements Runnable {

	public static final Log LOG = LogFactory.getLog(EventPublisher.class);
	
	public static final long MAX_SLEEP_MS = 60000;
	
	private final ConnectionPool connectionPool;
	private final ChannelPool channelPool;
	private final String exchangeName;
	private final String message;
	
	public EventPublisher(ConnectionPool connectionPool, ChannelPool channelPool,
			String exchangeName, String message) throws IOException {
		this.connectionPool = connectionPool;
		this.channelPool = channelPool;
		this.exchangeName = exchangeName;
		this.message = message;
	}

	@Override
	public void run() {
		for (long sleep = 100; sleep <= MAX_SLEEP_MS; sleep <<= 1) {
			try {
				Channel channel = channelPool.acquireChannel();
				channel.basicPublish(exchangeName, "", null, message.getBytes());
			} catch (InterruptedException e) {
				LOG.error("Thread could not sleep");
			} catch (IOException e) {
				LOG.error(e);
				try {
					Thread.sleep(sleep);
					Connection connection = connectionPool.acquireConnection();
					channelPool.addNewChannel(connection);
					connectionPool.releaseConnection(connection);
				} catch (InterruptedException e1) {
					LOG.error("Thread could not sleep");
				} catch (IOException e2) {
					LOG.error("Could not add new channel", e2);
				}
			}
		}
	}
	
}
