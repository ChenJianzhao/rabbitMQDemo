package org.demo.rabbitMQDemo.logQueues.topic;

import com.rabbitmq.client.*;

import java.io.IOException;

public class ReceiveLogsTopic {

  private static final String EXCHANGE_NAME = "topic_logs";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);
    String queueName = channel.queueDeclare().getQueue();

//    if (argv.length < 1) {
//      System.err.println("Usage: ReceiveLogsTopic [binding_key]...");
//      System.exit(1);
//    }

    /** 
     * 所有来源、所有等级的日志都需要打印到屏幕上 
     */
    String[] topics = new String[]{"#"};
    for (String bindingKey : topics) {
      channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
    }

    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
      }
    };
    channel.basicConsume(queueName, true, consumer);
  }
}