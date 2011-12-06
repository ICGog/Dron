package org.apache.hadoop.mapred.dron;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

public class ChannelPool {

	public static final Log LOG = LogFactory.getLog(ChannelPool.class);
	
	private final BlockingQueue<Channel> queue;
	
	public ChannelPool(Connection connection, int numChannels)
			throws InterruptedException, IOException {
		queue = new LinkedBlockingQueue<Channel>();
		for (int numChannel = 0; numChannel < numChannels; ++numChannel) {
			addNewChannel(connection);
		}
	}
	
	public Channel acquireChannel() throws InterruptedException {
		return queue.take();
	}
	
	public void releaseChannel(Channel channel) {
		try {
			queue.put(channel);
		} catch (InterruptedException e) {
			LOG.error("Thread interrupted", e);
		}
	}
	
	public void addNewChannel(Connection connection)
			throws IOException {
		try {
			queue.put(connection.createChannel());
		} catch (InterruptedException e) {
			LOG.error("Thread interrupted", e);
		}
	}
	
}
