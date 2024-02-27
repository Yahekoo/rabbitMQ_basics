package org.example.topics;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;


//We'll start off with a working assumption that the routing keys of logs will have two words:
// "<facility>.<severity>"
public class LogEmitter {
    private static final String EXCHANGE_NAME = "topic_logs";
    private static final String EXCHANGE_TYPE = "topic";



    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try(Connection connection = factory.newConnection()){
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
            String routingKey = getRouting(args);
            String message = getMessage(args);

            channel.basicPublish(EXCHANGE_NAME,routingKey,null,message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");

        } catch (Exception e) {
            //
        }

    }

    private static String getMessage(String[] args) {
        return args[1];
    }

    private static String getRouting(String[] args) {
        return args[0];
    }
}
