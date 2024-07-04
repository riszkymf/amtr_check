package dev.birudaun.antr.utils;

import dev.birudaun.antr.beans.Broker;
import dev.birudaun.antr.beans.Index;
import dev.birudaun.antr.beans.OrderBook;
import dev.birudaun.antr.beans.Stock;
import dev.birudaun.antr.beans.StockQuote;
import dev.birudaun.antr.beans.Trade;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility class for converting raw data strings into various data objects.
 */
public class AntrDataConverter {

  /**
   * Converts a raw data string into a StockQuote object.
   *
   * @param data The raw data string.
   * @return A StockQuote object.
   */
  public static StockQuote convertToStockQuote(String data) {
    String[] fields = StringUtils.split(data, "|");
    int index = 2;  // Skip the first two elements

    StockQuote quote = new StockQuote();
    quote.setDate(fields[index++]);
    quote.setTime(fields[index++]);
    quote.setSecurityCode(fields[index++]);
    quote.setBoardCode(fields[index++]);
    quote.setOpenPrice(Double.parseDouble(fields[index++]));
    quote.setHighPrice(Double.parseDouble(fields[index++]));
    quote.setLowPrice(Double.parseDouble(fields[index++]));
    quote.setClosePrice(Double.parseDouble(fields[index++]));
    quote.setVolume(Long.parseLong(fields[index++]));
    quote.setValue(Long.parseLong(fields[index++]));
    quote.setFreq(Long.parseLong(fields[index++]));
    quote.setPrevPrice(Double.parseDouble(fields[index++]));

    return quote;
  }

  /**
   * Converts a raw data string into a Trade object.
   *
   * @param data The raw data string.
   * @return A Trade object.
   */
  public static Trade convertToTrade(String data) {
    String[] fields = StringUtils.split(data, "|");
    int index = 2;  // Skip the first two elements

    Trade trade = new Trade();
    trade.setTradeDate(fields[index++]);
    trade.setTradeTime(fields[index++]);
    trade.setSecurityCode(fields[index++]);
    trade.setBoardCode(fields[index++]);
    trade.setTradeNo(fields[index++]);
    trade.setPrice(Double.parseDouble(fields[index++]));
    trade.setVolume(Long.parseLong(fields[index++]));

    return trade;
  }

  /**
   * Converts a raw data string into an Index object.
   *
   * @param data The raw data string.
   * @return An Index object.
   */
  public static Index convertToIndex(String data) {
    String[] fields = StringUtils.split(data, "|");
    int index = 2;  // Skip the first two elements

    Index indexData = new Index();
    indexData.setIndexCode(fields[index++]);
    indexData.setDate(fields[index++]);
    indexData.setTime(fields[index++]);
    indexData.setLastIndex(Double.parseDouble(fields[index++]));
    indexData.setPrevIndex(Double.parseDouble(fields[index++]));
    indexData.setOpenIndex(Double.parseDouble(fields[index++]));
    indexData.setHighIndex(Double.parseDouble(fields[index++]));
    indexData.setLowIndex(Double.parseDouble(fields[index++]));
    indexData.setChange(Double.parseDouble(fields[index++]));
    indexData.setChangeRate(Double.parseDouble(fields[index++]));
    indexData.setFreq(Long.parseLong(fields[index++]));
    indexData.setVolume(Long.parseLong(fields[index++]));
    indexData.setValue(Double.parseDouble(fields[index++]));
    indexData.setUp(Integer.parseInt(fields[index++]));
    indexData.setDown(Integer.parseInt(fields[index++]));
    indexData.setUnchange(Integer.parseInt(fields[index++]));
    indexData.setNoTransaction(Integer.parseInt(fields[index++]));
    indexData.setMarketValue(Double.parseDouble(fields[index++]));

    return indexData;
  }

  /**
   * Converts a raw data string into a Stock object.
   *
   * @param data The raw data string.
   * @return A Stock object.
   */
  public static Stock convertToStockData(String data) {
    String[] fields = StringUtils.split(data, "|");
    int index = 2;  // Skip the first two elements

    Stock stock = new Stock();
    stock.setSecurityCode(fields[index++]);
    stock.setSecurityName(fields[index++]);
    stock.setSecurityStatus(fields[index++]);
    stock.setSecurityType(fields[index++]);
    stock.setSubSector(fields[index++]);
    stock.setIpoPrice(Double.parseDouble(fields[index++]));
    stock.setBasePrice(Double.parseDouble(fields[index++]));
    stock.setListedShares(Integer.parseInt(fields[index++]));
    stock.setTradableListedShares(Integer.parseInt(fields[index++]));
    stock.setRemarks(fields[index++]);
    stock.setSecurityRemarks2(fields[index++]);
    stock.setWeight(Double.parseDouble(fields[index++]));

    return stock;
  }

  /**
   * Converts a raw data string into a Broker object.
   *
   * @param data The raw data string.
   * @return A Broker object.
   */
  public static Broker convertToBrokerData(String data) {
    String[] fields = StringUtils.split(data, "|");
    int index = 2;  // Skip the first two elements

    Broker broker = new Broker();
    broker.setBrokerCode(fields[index++]);
    broker.setBrokerName(fields[index++]);
    broker.setBrokerStatus(fields[index++]);

    return broker;
  }

  /**
   * Converts a raw data string into an OrderBook object for the best bid.
   *
   * @param data The raw data string.
   * @return An OrderBook object with the best bid data.
   */
  public static OrderBook convertToOrderBookBestBid(String data) {
    String[] fields = StringUtils.split(data, "|");
    int index = 2;  // Skip the first two elements

    OrderBook bestBid = new OrderBook();
    bestBid.setStockCode(fields[index++]);
    bestBid.setBidData(StringUtils.substring(data, 15));

    return bestBid;
  }

  /**
   * Converts a raw data string into an OrderBook object for the best offer.
   *
   * @param data The raw data string.
   * @return An OrderBook object with the best offer data.
   */
  public static OrderBook convertToOrderBookBestOffer(String data) {
    String[] fields = StringUtils.split(data, "|");
    int index = 2;  // Skip the first two elements

    OrderBook bestOffer = new OrderBook();
    bestOffer.setStockCode(fields[index++]);
    bestOffer.setOfferData(StringUtils.substring(data, 15));

    return bestOffer;
  }
}
