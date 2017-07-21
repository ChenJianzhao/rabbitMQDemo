package org.demo.rabbitMQDemo.workerQueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class WorkerRunner {

	public static void main(String[] args) {

		try {
			sendMsg(4);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		for(int i=0; i<2; i++) {
			
			new Thread(new Worker(),"Thread-"+ i).start();
		}
	}
	
	private static void sendMsg(int msgCount) throws Exception{
		
		String QUEUE_NAME = "hello";
		 
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost("localhost");
	    Connection connection = factory.newConnection();
	    Channel channel = connection.createChannel();

	    /** 
	     * 限制每个消费者同一时间只能消费一个消息 
	     * （即没有发送确认ack，不会向该消费者推送其他消息）
	     */
	    int prefetchCount = 1;
	    channel.basicQos(prefetchCount);
	    
	    // 定义队列
	    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
	    
	    for(int i=0; i<msgCount; i++) {
	    	String message = "[" + i + "] Hello World.";
	    	channel.basicPublish("", QUEUE_NAME, null, message.getBytes("UTF-8"));
	    	System.out.println(" ["+i+"] Sent '" + message + "'");
	    }

	    channel.close();
	    connection.close();
	}
}
