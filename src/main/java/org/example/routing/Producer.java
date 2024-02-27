package org.example.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;


//In this example we will emit logs based on their severity
//The Routing key will be the severity
public class Producer {
    private static final String EXCHANGE_NAME = "direct_logs";
    private static final String EXCHANGE_TYPE = "direct";



    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try(Connection connection = factory.newConnection()){
            Channel channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME,EXCHANGE_TYPE);
            String severity = getSeverity(args);
            String message = getMessage(args);

            channel.basicPublish(EXCHANGE_NAME,severity,null,message.getBytes(StandardCharsets.UTF_8));
            System.out.println(" [x] Sent '" + severity + "':'" + message + "'");

        } catch (Exception e) {
            //
        }

    }

    private static String getMessage(String[] args) {
        return args[1];
    }

    private static String getSeverity(String[] args) {
        return args[0];
    }
}
