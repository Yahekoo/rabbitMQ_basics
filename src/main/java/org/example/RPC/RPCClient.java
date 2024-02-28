package org.example.RPC;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class RPCClient implements AutoCloseable{

    private String QUEUEU_NAME = "rpc_queue";
    private Connection connection;
    private Channel channel;

    public RPCClient() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        this.connection = factory.newConnection();
        this.channel = connection.createChannel();
    }

    public static void main(String[] args) {
        try (RPCClient rpcClient = new RPCClient()) {
            for (int i = 0; i < 32; i++) {
                String i_str = Integer.toString(i);
                System.out.println(" [x] Requesting fib(" + i_str + ")");
                String response = rpcClient.call(i_str);
                System.out.println(" [.] Got '" + response + "'");
            }
        }catch (Exception e) {
            //
        }
    }

    public String call(String message) throws IOException, ExecutionException, InterruptedException {
        final String corrID = UUID.randomUUID().toString();
        String replyQueueName = channel.queueDeclare().getQueue();
        AMQP.BasicProperties properties = new AMQP.BasicProperties().builder()
                .correlationId(corrID)
                .replyTo(replyQueueName)
                .build();

        channel.basicPublish("",QUEUEU_NAME,properties,message.getBytes(StandardCharsets.UTF_8));

        //Suspend the Main Thread :
        final CompletableFuture<String> response = new CompletableFuture<>();
        DeliverCallback callback = (consTag, delivery) -> {
            if(delivery.getProperties().getCorrelationId().equals(corrID)) {
                response.complete(new String(delivery.getBody(),StandardCharsets.UTF_8));
            }
        };
        String cTag = channel.basicConsume(replyQueueName,false,callback, consTag -> {});
        String result = response.get();
        System.out.println("cTag is ====>"+cTag);
        channel.basicCancel(cTag);
        return result;
    }


    @Override
    public void close() throws Exception {
        this.connection.close();
    }
}
