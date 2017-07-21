package org.demo.rabbitMQDemo.logQueues.topic;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class ReceiveLogsTopicToDisk {

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
     * both severity (info/warn/critical...) and facility (auth/cron/kern...).
     * 所有 from kern and critical severity 的日志会保存到日志文件中
     */
    String[] topics = new String[]{"kern.*","*.critical"};
    for (String bindingKey : topics) {
      channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
    }

    System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

    Consumer consumer = new DefaultConsumer(channel) {
      @Override
      public void handleDelivery(String consumerTag, Envelope envelope,
                                 AMQP.BasicProperties properties, byte[] body) throws IOException {
        String message = new String(body, "UTF-8");
        message = " [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'" + "\n";
        FileOutputStream file = null;
        try{
        	file = new FileOutputStream("./topic.log",true);
        	file.write(message.getBytes("UTF-8"));
        }catch(FileNotFoundException e){
        	e.printStackTrace();
        }finally{
        	file.close();
        }
        
//        System.out.println(" [x] Received '" + envelope.getRoutingKey() + "':'" + message + "'");
      }
    };
    channel.basicConsume(queueName, true, consumer);
  }
}