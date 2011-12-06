package org.apache.hadoop.mapred.dron;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class Dron {

	// TODO(ionel): Move hardcoded values into conf file.
	private static final String HOST = "localhost";
	private static final int NUM_CONNECTIONS = 1;
	private static final int NUM_CHANNELS = 3;
	// NUM_PUBLISH_THREADS must be <= NUM_CHANNELS.
	private static final int NUM_PUBLISH_THREADS = 3;
	
	private final ConnectionPool connectionPool;
	private final ChannelPool channelPool;
	private final ExecutorService executorService;
	
	public Dron() throws InterruptedException, IOException {
		connectionPool = new ConnectionPool(HOST, NUM_CONNECTIONS);
		Connection connection = connectionPool.acquireConnection();
		channelPool = new ChannelPool(connection, NUM_CHANNELS);
		connectionPool.releaseConnection(connection);
		executorService = Executors.newFixedThreadPool(NUM_PUBLISH_THREADS);
	}
	
	public void publish(String exchangeName, String message) throws IOException {
		executorService.execute(new EventPublisher(connectionPool, channelPool,
				exchangeName, message));
	}
	
	public void declareExchange(String exchangeName, String type)
			throws InterruptedException, IOException {
		Channel channel = channelPool.acquireChannel();
		channel.exchangeDeclare(exchangeName, type);
		channelPool.releaseChannel(channel);
	}
	
}
