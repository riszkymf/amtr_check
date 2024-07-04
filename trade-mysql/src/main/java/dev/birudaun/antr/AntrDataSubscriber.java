package dev.birudaun.antr;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import dev.birudaun.antr.db.HsqldbManager;
import dev.birudaun.antr.db.HsqldbOperation;
import dev.birudaun.antr.utils.ApplicationProperties;
import java.io.IOException;
import java.util.Properties;

public class AntrDataSubscriber {
  private static final String EXCHANGE_IDXDATA = "idxdata";
  
  private static final String EXCHANGE_ITCHDATA = "itchdata";
  
  public static void main(String[] args) throws Exception {}
  
  public AntrDataSubscriber() throws Exception {
    Properties props = ApplicationProperties.getInstance().getProperties();
    System.out.println(props.toString());
    try {
      HsqldbManager manager = new HsqldbManager();
      manager.startServer();
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
    HsqldbOperation.createTables();
    ConnectionFactory factory = new ConnectionFactory();
    String url = System.getenv("RMQ_URL");
    String username = System.getenv("RMQ_UN");
    String password = System.getenv("RMQ_PS");
    factory.setHost(url);
    factory.setUsername(username);
    factory.setPassword(password);
    Connection connection = factory.newConnection();
    Channel channelIdxData = connection.createChannel();
    channelIdxData.exchangeDeclare("idxdata", "fanout");
    String queueIdxData = channelIdxData.queueDeclare().getQueue();
    channelIdxData.queueBind(queueIdxData, "idxdata", "");
    System.out.println(" [*] Waiting for IDXDATA messages. To exit press CTRL+C");
    DeliverCallback dcIdxData = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        processMessage(message);
      };
    channelIdxData.basicConsume(queueIdxData, true, dcIdxData, consumerTag -> {
        
        });
    Channel channelItchData = connection.createChannel();
    channelItchData.exchangeDeclare("itchdata", "fanout");
    String queueItchData = channelItchData.queueDeclare().getQueue();
    channelItchData.queueBind(queueItchData, "itchdata", "");
    System.out.println(" [*] Waiting for ITCHDATA messages. To exit press CTRL+C");
    DeliverCallback dcItchData = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        processMessage(message);
      };
    channelItchData.basicConsume(queueItchData, true, dcItchData, consumerTag -> {
        
        });
  }
  
  public void processMessage(String line) {
    try {
      char recordType = line.charAt(5);
      switch (recordType) {
        case '5':
          System.out.println(line);
          break;
        case '6':
          System.out.println(line);
          break;
        case '7':
          System.out.println(line);
          break;
        case '8':
          System.out.println("eod: " + line);
          break;
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
  }
}
