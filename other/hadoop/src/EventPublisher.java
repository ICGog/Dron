package org.apache.hadoop.mapred;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class EventPublisher {

	private final Connection connection;
	private final Channel channel;
	
	public EventPublisher(String host) throws IOException {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(host);
		connection = factory.newConnection();
		channel = connection.createChannel();
	}
	
	public void declareExchange(String exchangeName, String type)
			throws IOException {
		channel.exchangeDeclare(exchangeName, type);
	}
	
	public void publishMessage(String exchangeName, String message)
			throws IOException {
		channel.basicPublish(exchangeName, "", null, message.getBytes());		
	}
	
	public void close() throws IOException {
		channel.close();
		connection.close();
	}
	
}
