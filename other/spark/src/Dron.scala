package spark

import java.io.IOException
import com.rabbitmq.client.{Channel, Connection, ConnectionFactory}

class Dron(channel: Channel) {

  private val exchangeName: String = "dron_events"

  def publishMessage(message: String) {
    channel.basicPublish(exchangeName, "", null, message.getBytes())
  }

}