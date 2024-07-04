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
/**
 * Subscriber for consuming messages from RabbitMQ exchanges (idxdata and itchdata),
 * processing them based on record types, and storing data into MySQL database.
 */
public class MysqlDataSubscriber {
  private static final String EXCHANGE_IDXDATA = "idxdata";
  private static final String EXCHANGE_ITCHDATA = "itchdata";

  /**
   * Main method to initialize the MysqlDataSubscriber instance.
   *
   * @param args Command-line arguments (not used).
   * @throws Exception If any error occurs during initialization.
   */
  public static void main(String[] args) throws Exception {
    MysqlDataSubscriber subscriber = new MysqlDataSubscriber();
  }

  /**
   * Constructor for MysqlDataSubscriber. Initializes RabbitMQ connection,
   * creates channels for consuming messages from idxdata and itchdata exchanges,
   * and binds queues to these exchanges.
   *
   * @throws Exception If any error occurs during RabbitMQ setup.
   */
  public MysqlDataSubscriber() throws Exception {
    // Load application properties
    Properties props = ApplicationProperties.getInstance().getProperties();
    System.out.println(props.toString());

    // Establish RabbitMQ connection
    ConnectionFactory factory = new ConnectionFactory();
    String url = System.getenv("RMQ_URL");
    String username = System.getenv("RMQ_UN");
    String password = System.getenv("RMQ_PS");
    factory.setHost(url);
    factory.setUsername(username);
    factory.setPassword(password);
    Connection connection = factory.newConnection();

    // Create channel for IDXDATA messages
    Channel channelIdxData = connection.createChannel();
    channelIdxData.exchangeDeclare(EXCHANGE_IDXDATA, "fanout");
    String queueIdxData = channelIdxData.queueDeclare().getQueue();
    channelIdxData.queueBind(queueIdxData, EXCHANGE_IDXDATA, "");
    System.out.println(" [*] Waiting for IDXDATA messages. To exit press CTRL+C");

    // Consume messages from IDXDATA queue
    DeliverCallback dcIdxData = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), "UTF-8");
      processMessage(message);
    };
    channelIdxData.basicConsume(queueIdxData, true, dcIdxData, consumerTag -> {});

    // Create channel for ITCHDATA messages
    Channel channelItchData = connection.createChannel();
    channelItchData.exchangeDeclare(EXCHANGE_ITCHDATA, "fanout");
    String queueItchData = channelItchData.queueDeclare().getQueue();
    channelItchData.queueBind(queueItchData, EXCHANGE_ITCHDATA, "");
    System.out.println(" [*] Waiting for ITCHDATA messages. To exit press CTRL+C");

    // Consume messages from ITCHDATA queue
    DeliverCallback dcItchData = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), "UTF-8");
      processMessage(message);
    };
    channelItchData.basicConsume(queueItchData, true, dcItchData, consumerTag -> {});
  }

  /**
   * Processes incoming message based on its record type, converts it to respective
   * data object (Trade, StockQuote, Index, Stock), and saves it into MySQL database.
   *
   * @param line Message received from RabbitMQ.
   */
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