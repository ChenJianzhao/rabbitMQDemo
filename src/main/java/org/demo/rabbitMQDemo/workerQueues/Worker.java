package org.demo.rabbitMQDemo.workerQueues;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class Worker implements Runnable{

	private final static String TASK_QUEUE_NAME = "hello";

	public void run() {
		
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = null;
//		channel = null;
		Consumer consumer = null;
		
		try {
			connection = factory.newConnection();
			final Channel channel= connection.createChannel();

			int prefetchCount = 1;
			channel.basicQos(prefetchCount);
			
			channel.queueDeclare(TASK_QUEUE_NAME, false, false, false, null);
			System.out.println(" [*] Waiting for messages. To exit press CTRL+C");
			  
			consumer = new DefaultConsumer(channel) {
				@Override
				public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
			    String message = new String(body, "UTF-8");
		
			    System.out.println(" ["+ Thread.currentThread().getName() +"] Received '" + message + "'");
			    try {
			      doWork(message);
			    } catch(InterruptedException e){
			    	e.printStackTrace();
			    }finally {
			      System.out.println(" [x] Done");
			      /**
			       * 向服务器发送确认消息
			       */
			      channel.basicAck(envelope.getDeliveryTag(), false);
			    }
			  }
			};
			
			/**
			 *  autoAck 是否自动发送确认消息
			 *  消费者没有确认的情况下，RabbitMQ Server不会删除该消息，也不会再向该消费者再次推送同一消息
			 */
//			boolean autoAck = true; // acknowledgment is covered below
			boolean autoAck = false;
			channel.basicConsume(TASK_QUEUE_NAME, autoAck, consumer);
			
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	} 
		
	
	private static void doWork(String task) throws InterruptedException {
	    for (char ch: task.toCharArray()) {
	        if (ch == '.') Thread.sleep(1000);
	    }
	}
	
}