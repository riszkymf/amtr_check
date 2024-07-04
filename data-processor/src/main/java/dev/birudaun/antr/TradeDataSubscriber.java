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

/**
 * This class subscribes to trade data messages from RabbitMQ and processes them based on their record type.
 */
public class TradeDataSubscriber {
  private static final String EXCHANGE_IDXDATA = "idxdata";
  private static String dbUse = "hsqldb"; // Default database type

  /**
   * Constructor that initializes the subscriber and sets up necessary connections.
   * @throws Exception if there's an issue during initialization
   */
  public TradeDataSubscriber() throws Exception {
    // Load application properties
    Properties props = ApplicationProperties.getInstance().getProperties();
    System.out.println(props.toString());

    // Determine which database to use based on configuration
    dbUse = props.getProperty("db.use", "hsqldb");

    // Initialize HSQLDB if configured
    if ("hsqldb".equals(dbUse)) {
      try {
        HsqldbManager manager = new HsqldbManager();
        manager.startServer();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
      HsqldbOperation.createTables(); // Create necessary tables in HSQLDB
    }

    // Setup RabbitMQ connection
    ConnectionFactory factory = new ConnectionFactory();
    String url = props.getProperty("rmq.url");
    String username = props.getProperty("rmq.un", "admin");
    String password = props.getProperty("rmq.ps");
    factory.setHost(url);
    factory.setUsername(username);
    factory.setPassword(password);

    // Create connection and channel for IDXDATA exchange
    Connection connection = factory.newConnection();
    Channel channelIdxData = connection.createChannel();
    channelIdxData.exchangeDeclare("idxdata", "fanout"); // Declare a fanout exchange
    String queueIdxData = channelIdxData.queueDeclare().getQueue();
    channelIdxData.queueBind(queueIdxData, "idxdata", ""); // Bind queue to exchange

    System.out.println(" [*] Waiting for IDXDATA messages. To exit press CTRL+C");

    // Callback for processing messages received from RabbitMQ
    DeliverCallback dcIdxData = (consumerTag, delivery) -> {
      String message = new String(delivery.getBody(), "UTF-8");
      processMessage(message); // Process received message
    };

    // Start consuming messages from the queue
    channelIdxData.basicConsume(queueIdxData, true, dcIdxData, consumerTag -> {
      // Empty callback for consumer cancellation
    });
  }

  /**
   * Processes a received message based on its record type.
   * @param line the message to process
   */
  public void processMessage(String line) {
    try {
      Trade trade;
      char recordType = line.charAt(5); // Extract record type from the message

      System.out.println(recordType); // Print record type for debugging

      // Process the message based on record type
      switch (recordType) {
        case '1':
          trade = AntrDataConverter.convertToTrade(line); // Convert message to Trade object
          if ("hsqldb".equals(dbUse)) {
            HsqldbOperation.saveTrade(trade); // Save Trade object to HSQLDB
          } else {
            MysqlDbOperation.saveTrade(trade); // Save Trade object to MySQL
          }
          break;
        // Add cases for other record types as needed
        default:
          System.out.println("Unhandled record type: " + recordType);
          break;
      }
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Main method to start the TradeDataSubscriber.
   * @param args command-line arguments (not used)
   * @throws Exception if there's an issue during execution
   */
  public static void main(String[] args) throws Exception {
    new TradeDataSubscriber(); // Initialize and start the subscriber
  }
}