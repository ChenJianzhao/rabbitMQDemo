package org.demo.rabbitMQDemo.logQueues.direct;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class ReceiveLogsDirectToDisk {

  private static final String EXCHANGE_NAME = "direct_logs";

  public static void main(String[] argv) throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    Connection connection = factory.newConnection();
    Channel channel = connection.createChannel();

    channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);

    /** 无参数定义一个非持久、独占、自动删除、随机命名的队列 */
    String queueName = channel.queueDeclare().getQueue();


    String[] severitys = new String[]{"error"};
    for(String severity : severitys){
      channel.queueBind(queueName, EXCHANGE_NAME, severity);
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
        	file = new FileOutputStream("./direct.log",true);
        	file.write(message.getBytes("UTF-8"));
        }catch(FileNotFoundException e){
        	e.printStackTrace();
        }finally{
        	file.close();
        }
        
      }
    };
    channel.basicConsume(queueName, true, consumer);
  }
}