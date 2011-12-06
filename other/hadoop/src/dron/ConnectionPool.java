package org.apache.hadoop.mapred.dron;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class ConnectionPool {

	public static final Log LOG = LogFactory.getLog(ConnectionPool.class);
	
	private final BlockingQueue<Connection> queue;
	private final ConnectionFactory connectionFactory;
	
	public ConnectionPool(String host, int numConnections) throws IOException {
		queue = new LinkedBlockingQueue<Connection>();
		connectionFactory = new ConnectionFactory();
		connectionFactory.setHost(host);
		for (int numConnection = 0; numConnection < numConnections;
				++numConnection) {
			addNewConnection();
		}
	}

	public Connection acquireConnection() throws InterruptedException {
		return queue.take();
	}
	
	public void releaseConnection(Connection connection) {
		try {
			queue.put(connection);
		} catch (InterruptedException e) {
			LOG.error("Thread interrupted", e);
		}
	}
	
	public void addNewConnection() throws IOException {
		try {
			queue.put(connectionFactory.newConnection());
		} catch (InterruptedException e) {
			LOG.error("Thread interrupted", e);
		}
	}
	
}
