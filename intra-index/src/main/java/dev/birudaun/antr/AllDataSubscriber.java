package dev.birudaun.antr;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import dev.birudaun.antr.beans.Broker;
import dev.birudaun.antr.beans.Index;
import dev.birudaun.antr.beans.Stock;
import dev.birudaun.antr.beans.StockQuote;
import dev.birudaun.antr.beans.Trade;
import dev.birudaun.antr.db.MysqlDbOperation;
import dev.birudaun.antr.utils.AntrDataConverter;
import dev.birudaun.antr.utils.ApplicationProperties;
import java.io.IOException;
import java.util.Properties;

/**
 * Entry point for the AllDataSubscriber application.
 * This class subscribes to two RabbitMQ exchanges: "idxdata" and "itchdata".
 * It processes incoming messages from these exchanges and saves the data into a MySQL database.
 */
public class AllDataSubscriber {
  private static final String EXCHANGE_IDXDATA = "idxdata";
  private static final String EXCHANGE_ITCHDATA = "itchdata";

  /**
   * Main method to run the AllDataSubscriber application.
   * @param args Command-line arguments.
   * @throws Exception If an error occurs while setting up the subscriber.
   */
  public static void main(String[] args) throws Exception {
    AllDataSubscriber subscriber = new AllDataSubscriber();
  }

  /**
   * Constructor for AllDataSubscriber.
   * Initializes connections to RabbitMQ exchanges and sets up message consumers.
   * @throws Exception If an error occurs while setting up the connections.
   */
  public AllDataSubscriber() throws Exception {
    // Load application properties
    Properties props = ApplicationProperties.getInstance().getProperties();
    System.out.println(props.toString());

    // Set up RabbitMQ connection
    ConnectionFactory factory = new ConnectionFactory();
    String url = props.getProperty("rmq.url");
    String username = props.getProperty("rmq.un", "admin");
    String password = props.getProperty("rmq.ps");
    factory.setHost(url);
    factory.setUsername(username);
    factory.setPassword(password);
    Connection connection = factory.newConnection();

    // Set up IDX data channel and consumer
    Channel channelIdxData = connection.createChannel();
    channelIdxData.exchangeDeclare(EXCHANGE_IDXDATA, "fanout");
    String queueIdxData = channelIdxData.queueDeclare().getQueue();
    channelIdxData.queueBind(queueIdxData, EXCHANGE_IDXDATA, "");
    System.out.println(" [*] Waiting for IDXDATA messages. To exit press CTRL+C");
    DeliverCallback dcIdxData = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), "UTF-8");
      processMessage(message);
    };
    channelIdxData.basicConsume(queueIdxData, true, dcIdxData, consumerTag -> {});

    // Set up ITCH data channel and consumer
    Channel channelItchData = connection.createChannel();
    channelItchData.exchangeDeclare(EXCHANGE_ITCHDATA, "fanout");
    String queueItchData = channelItchData.queueDeclare().getQueue();
    channelItchData.queueBind(queueItchData, EXCHANGE_ITCHDATA, "");
    System.out.println(" [*] Waiting for ITCHDATA messages. To exit press CTRL+C");
    DeliverCallback dcItchData = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), "UTF-8");
      processMessage(message);
    };
    channelItchData.basicConsume(queueItchData, true, dcItchData, consumerTag -> {});
  }

  /**
   * Processes incoming messages and saves the data to the MySQL database.
   * @param line The message line received from the RabbitMQ exchange.
   */
  public void processMessage(String line) {
    try {
      char recordType = line.charAt(5);
      System.out.println(recordType);
      switch (recordType) {
        case '1': // Trade data
          Trade trade = AntrDataConverter.convertToTrade(line);
          MysqlDbOperation.saveTrade(trade);
          break;
        case '2': // Stock quote data
          StockQuote quote = AntrDataConverter.convertToStockQuote(line);
          MysqlDbOperation.saveStockQuote(quote);
          break;
        case '9': // Index data
          Index index = AntrDataConverter.convertToIndex(line);
          MysqlDbOperation.saveIndex(index);
          MysqlDbOperation.saveIntradayIndex(index);
          break;
        case 'A': // Stock data
          Stock stock = AntrDataConverter.convertToStockData(line);
          MysqlDbOperation.saveStockData(stock);
          break;
        case 'B': // Broker data
          Broker broker = AntrDataConverter.convertToBrokerData(line);
          MysqlDbOperation.saveBrokerData(broker);
          break;
        default:
          System.out.println("Unknown record type: " + recordType);
          break;
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }
}