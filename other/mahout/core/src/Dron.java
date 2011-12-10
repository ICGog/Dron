package org.apache.mahout.driver;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.json.simple.JSONObject;

public class Dron {

  private final Connection connection;
  private final Channel channel;

  public Dron(String host) throws IOException {
    ConnectionFactory connectionFactory = new ConnectionFactory();
    connectionFactory.setHost(host);
    connection = connectionFactory.newConnection();
    channel = connection.createChannel();
  }

  public void publish(String exchangeName, String message)
      throws IOException {
    channel.basicPublish(exchangeName, "", null, message.getBytes());
  }

  public void close() throws IOException {
    channel.close();
    connection.close();
  }

  public String buildJsonString(String jobId, String state) {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("job_instance", jobId);
    jsonObject.put("state", state);
    return jsonObject.toJSONString();
  }

}