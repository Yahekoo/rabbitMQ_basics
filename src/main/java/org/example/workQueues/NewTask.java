package org.example.workQueues;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class NewTask {

    private static final String QUEUE_NAME = "NEW_TASK_QUEUE";

    public static void main(String[] args) {
        String message = String.join(" ",args);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try(Connection connection = factory.newConnection()) {
            Channel channel = connection.createChannel();
            // The Queueu will be saved even when the broker gets down
            boolean durability = true;
            channel.queueDeclare(QUEUE_NAME,durability,false,false,null);
            //Make the message persistent
            channel.basicPublish("",QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }

    }

}
