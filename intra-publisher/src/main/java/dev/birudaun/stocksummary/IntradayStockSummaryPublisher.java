package dev.birudaun.stocksummary;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import dev.birudaun.stocksummary.beans.Trade;
import dev.birudaun.stocksummary.db.HsqldbOperation;
import dev.birudaun.utils.ApplicationProperties;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;



/**
 * IntradayStockSummaryPublisher is responsible for publishing intraday stock summary data
 * to a RabbitMQ exchange based on incoming trade messages.
 */
public class IntradayStockSummaryPublisher {

  /**
   * Channel for publishing messages to RabbitMQ.
   */
  private Channel pubChannel;

  /**
   * Name of the exchange for publishing messages.
   */
  private String pubExchangeName;

  /**
   * Properties for configuring RabbitMQ connections.
   */
  private Properties props;

  /**
   * Name of the exchange for incoming data.
   */
  private static final String EXCHANGE_IDXDATA = "idxdata";

  /**
   * Flag to indicate if it's the end of a minute.
   */
  private boolean isEndOfMinute = false;

  /**
   * Last trade time processed.
   */
  private String lastTradeTime;

  /**
   * Initializes an instance of IntradayStockSummaryPublisher by setting up
   * connections and channels for both publishing and subscribing to RabbitMQ exchanges.
   *
   * @throws Exception if any error occurs during setup.
   */
  public IntradayStockSummaryPublisher() throws Exception {
    // Initialize properties
    this.props = ApplicationProperties.getInstance().getProperties();

    // Setting up RabbitMQ connection for publishing
    ConnectionFactory pubFactory = new ConnectionFactory();
    pubFactory.setHost(this.props.getProperty("rabbitmq.host"));
    pubFactory.setUsername(this.props.getProperty("rabbitmq.username"));
    pubFactory.setPassword(this.props.getProperty("rabbitmq.password"));
    this.pubExchangeName = this.props.getProperty("rabbitmq.exchange_name");
    Connection pubConnection = pubFactory.newConnection();
    this.pubChannel = pubConnection.createChannel();
    this.pubChannel.exchangeDeclare(this.pubExchangeName, "direct");
    System.out.println("Connected to exchange: " + this.pubExchangeName);

    // Setting up RabbitMQ connection for subscribing
    ConnectionFactory subFactory = new ConnectionFactory();
    subFactory.setHost(this.props.getProperty("rabbitmq.subs.host"));
    subFactory.setUsername(this.props.getProperty("rabbitmq.subs.username", "admin"));
    subFactory.setPassword(this.props.getProperty("rabbitmq.subs.password"));
    Connection subConnection = subFactory.newConnection();
    Channel subChannel = subConnection.createChannel();
    subChannel.exchangeDeclare("idxdata", "fanout");
    String queueIdxData = subChannel.queueDeclare().getQueue();
    subChannel.queueBind(queueIdxData, "idxdata", "");
    System.out.println(" [*] Waiting for IDXDATA messages. To exit press CTRL+C");

    // Callback function for consuming messages
    DeliverCallback dcIdxData = (consumerTag, delivery) -> {
      Trade trade;
      String message = new String(delivery.getBody(), "UTF-8");
      char recordType = message.charAt(5);
      switch (recordType) {
        case '1':
          trade = convertToTrade(message);
          if (trade.getTradeTime().endsWith("00") && !this.isEndOfMinute) {
            this.isEndOfMinute = true;
            System.out.println("\t" + trade.toString() + "\t" + this.lastTradeTime);
            List<String> results = HsqldbOperation.getIntradayStockSummary(this.lastTradeTime);
            for (String result : results) {
              publishMessage(result.getBytes());
            }
            System.out.println("\t" + (new SimpleDateFormat("HH:mm:ss")).format(new Date()) + ": successfully publish intraday data... ");
            break;
          }
          if (!trade.getTradeTime().endsWith("00")) {
            this.lastTradeTime = trade.getTradeTime().substring(0, 4);
            this.isEndOfMinute = false;
          }
          break;
      }
    };
    // Start consuming messages
    subChannel.basicConsume(queueIdxData, true, dcIdxData, consumerTag -> {});
  }
  /**
   * Publishes a message to the RabbitMQ exchange.
   *
   * @param message the message to publish.
   */
  public void publishMessage(byte[] message) {
    try {
      this.pubChannel.basicPublish(
              this.pubExchangeName,
              this.props.getProperty("rabbitmq.routing_key"),
              null,
              message);
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  /**
   * Converts a message string into a Trade object.
   *
   * @param data the message string to convert.
   * @return the Trade object.
   */
  public static Trade convertToTrade(String data) {
    // split the string
    String[] datas = StringUtils.split(data, "|");
    // initialize start index. This will start extracting data from the third element of the datas array
    int x = 2;

    // Possible format UNKNOWN | UNKNOWN | trade_date | trade_time | security_code | board_coe | trade_no | price | volume

    Trade trade = new Trade();
    trade.setTradeDate(datas[x++]);
    trade.setTradeTime(datas[x++]);
    trade.setSecurityCode(datas[x++]);
    trade.setBoardCode(datas[x++]);
    trade.setTradeNo(datas[x++]);
    trade.setPrice(Double.parseDouble(datas[x++]));
    trade.setVolume(Long.parseLong(datas[x++]));
    return trade;
  }

  /**
   * Main method to create an instance of IntradayStockSummaryPublisher,
   * initializing the process.
   *
   * @param args the command line arguments.
   * @throws Exception if any error occurs during initialization.
   */
  public static void main(String[] args) throws Exception {
    IntradayStockSummaryPublisher publisher = new IntradayStockSummaryPublisher();
  }
}

