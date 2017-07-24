package org.demo.rabbitMQDemo.workerQueues;

/**
 * Created by cjz on 2017/7/24.
 */
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class NewTask {

    private static final String TASK_QUEUE_NAME = "task_queue";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(TASK_QUEUE_NAME, true, false, false, null);

        int msgCount = 0;
        String message = "[" + msgCount + "] Hello World.";
        System.out.println(" ["+ msgCount +"] Sent '" + message + "'");

        channel.basicPublish("", TASK_QUEUE_NAME,
                MessageProperties.PERSISTENT_TEXT_PLAIN,
                message.getBytes("UTF-8"));
//        System.out.println(" [x] Sent '" + message + "'");

        channel.close();
        connection.close();
    }
}