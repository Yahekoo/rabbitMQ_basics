package org.example.RPC;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class RPCServer {
    private static final String QUEUE_NAME = "rpc_queue";

    public static int fibonacci(int n) {
        if (n == 0) return 0;
        if (n == 1) return 1;
        return fibonacci(n - 1) + fibonacci(n - 2);
    }

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queuePurge(QUEUE_NAME);

        channel.basicQos(1);

        System.out.println(" [x] Server Awaiting RPC requests");
        System.out.println("================================================");

        DeliverCallback callback = (consTag, delivery) -> {
            System.out.println("===> ConsTag is : " + consTag);
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .correlationId(delivery.getProperties().getCorrelationId())
                    .build();

            String response = "";
            try {
                String message = new String(delivery.getBody(), "UTF-8");
                int i = Integer.parseInt(message);
                System.out.println(" [.] fib(" + message + ")");
                response += fibonacci(i);
            } catch (RuntimeException e) {
                System.out.println(" [.] " + e);
            } finally {
                channel.basicPublish("", delivery.getProperties().getReplyTo(), properties, response.getBytes(StandardCharsets.UTF_8));
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };

        //False for AutoAck
        channel.basicConsume(QUEUE_NAME,false,callback,consTag -> {});

    }
}