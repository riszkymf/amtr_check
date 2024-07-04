package dev.birudaun.antr;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import dev.birudaun.antr.beans.Trade;
import dev.birudaun.antr.db.HsqldbManager;
import dev.birudaun.antr.db.HsqldbOperation;
import dev.birudaun.antr.db.MysqlDbOperation;
import dev.birudaun.antr.utils.AntrDataConverter;
import dev.birudaun.antr.utils.ApplicationProperties;
import java.io.IOException;
import java.util.Properties;

public class TradeDataSubscriber {
  private static final String EXCHANGE_IDXDATA = "idxdata";
  
  private static String dbUse = "hsqldb";
  
  public static void main(String[] args) throws Exception {}
  
  public TradeDataSubscriber() throws Exception {
    Properties props = ApplicationProperties.getInstance().getProperties();
    System.out.println(props.toString());
    dbUse = props.getProperty("db.use", "hsqldb");
    if ("hsqldb".equals(dbUse)) {
      try {
        HsqldbManager manager = new HsqldbManager();
        manager.startServer();
      } catch (Exception ex) {
        ex.printStackTrace();
      } 
      HsqldbOperation.createTables();
    } 
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
  }
  
  public void processMessage(String line) {
    try {
      Trade trade;
      char recordType = line.charAt(5);
      System.out.println(recordType);
      switch (recordType) {
        case '1':
          trade = AntrDataConverter.convertToTrade(line);
          if ("hsqldb".equals(dbUse)) {
            HsqldbOperation.saveTrade(trade);
            break;
          } 
          MysqlDbOperation.saveTrade(trade);
          break;
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
  }
}
