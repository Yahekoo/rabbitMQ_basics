package org.example.routing;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

//This is a Log Receiver
// We create a new binding for each severity
public class Consumer {
    private static final String EXCHANGE_NAME = "direct_logs";
    private static final String EXCHANGE_TYPE = "direct";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
        //a random generated queue by Rabbit -- It is temporary
        String queue = channel.queueDeclare().getQueue();
        if (args.length < 1) {
            System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
            System.exit(1);
        }
        //BINDING ------------------

        for(String severity : args) {
            channel.queueBind(queue,EXCHANGE_NAME,severity);
        }

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received : "+delivery.getEnvelope().getRoutingKey()+" '" + message + "'");
        };

        channel.basicConsume(queue, true, deliverCallback, consumerTag -> { });

    }
}

