package dev.birudaun.antr;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import dev.birudaun.antr.beans.Index;
import dev.birudaun.antr.beans.Stock;
import dev.birudaun.antr.beans.StockQuote;
import dev.birudaun.antr.beans.Trade;
import dev.birudaun.antr.db.MysqlDbOperation;
import dev.birudaun.antr.utils.AntrDataConverter;
import dev.birudaun.antr.utils.ApplicationProperties;
import java.io.IOException;
import java.util.Properties;

public class MysqlDataSubscriber {
  private static final String EXCHANGE_IDXDATA = "idxdata";
  
  private static final String EXCHANGE_ITCHDATA = "itchdata";
  
  public static void main(String[] args) throws Exception {}
  
  public MysqlDataSubscriber() throws Exception {
    Properties props = ApplicationProperties.getInstance().getProperties();
    System.out.println(props.toString());
    ConnectionFactory factory = new ConnectionFactory();
    String url = props.getProperty("rmq.url");
    String username = props.getProperty("rmq.un", "admin");
    String password = props.getProperty("rmq.ps");
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
      Trade trade;
      StockQuote quote;
      Index index;
      Stock stock;
      char recordType = line.charAt(5);
      switch (recordType) {
        case '1':
          trade = AntrDataConverter.convertToTrade(line);
          System.out.println(recordType);
          MysqlDbOperation.saveTrade(trade);
          break;
        case '2':
          quote = AntrDataConverter.convertToStockQuote(line);
          MysqlDbOperation.saveStockQuote(quote);
          break;
        case '9':
          index = AntrDataConverter.convertToIndex(line);
          MysqlDbOperation.saveIndex(index);
          break;
        case 'A':
          stock = AntrDataConverter.convertToStockData(line);
          MysqlDbOperation.saveStockData(stock);
          break;
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
  }
}
