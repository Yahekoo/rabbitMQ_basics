package org.example.workQueues;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;


//CONSUMERS_HERE_USE_ROUND_ROBIN
//Means Each task is delivered to only one worker
public class Worker {
    private static final String QUEUE_NAME = "NEW_TASK_QUEUE";

    public static void doWork(String message) throws InterruptedException {
        for(char c : message.toCharArray()) {
            if(c == '.') {
                Thread.sleep(1000);
            }
        }
    };

    public static void main(String[] args) throws IOException, TimeoutException {


        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        //A worker got 1 message at a time, if the worker is busy he wont receive messages waiting
        //FAIR DISPATCH
        channel.basicQos(1);
        boolean durability = true;
        channel.queueDeclare(QUEUE_NAME,durability,false,false,null);
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [X] Received '" + message +"'");
            try {
                doWork(message);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                System.out.println(" [x] Done");
            }
        };

        //Auto Acknowledge

        boolean auto_ack = true;

        channel.basicConsume(QUEUE_NAME,auto_ack,deliverCallback,consTag -> {});


    }
}
