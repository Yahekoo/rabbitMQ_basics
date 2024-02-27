package org.example.rabbitMQ;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Send {

    private static final String QUEUE_NAME = "hello";

    public static void main(String[] args) {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try(Connection connection =  factory.newConnection()) {
            Channel channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME,false,false,false,null);
            String message = "Hello World !";
            //Empty String for Default Exchange
            channel.basicPublish("",QUEUE_NAME,null,message.getBytes());
            System.out.println(" [X] sent '"+message+"'");

        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
