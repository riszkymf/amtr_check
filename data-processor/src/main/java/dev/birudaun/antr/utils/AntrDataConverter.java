package dev.birudaun.antr.utils;

import dev.birudaun.antr.beans.Broker;
import dev.birudaun.antr.beans.Index;
import dev.birudaun.antr.beans.OrderBook;
import dev.birudaun.antr.beans.Stock;
import dev.birudaun.antr.beans.StockQuote;
import dev.birudaun.antr.beans.Trade;
import org.apache.commons.lang3.StringUtils;

/**
 * AntrDataConverter class converts string data to specific data objects such as StockQuote, Trade, Index, etc.
 */
public class AntrDataConverter {

  /**
   * Converts a string to a StockQuote object.
   *
   * @param data the string data received from the broker.
   * @return a StockQuote object with parsed data.
   */
  public static StockQuote convertToStockQuote(String data) {
    String[] datas = StringUtils.split(data, "|");
    int x = 2;
    StockQuote quote = new StockQuote();
    quote.setDate(datas[x++]);
    quote.setTime(datas[x++]);
    quote.setSecurityCode(datas[x++]);
    quote.setBoardCode(datas[x++]);
    quote.setOpenPrice(Double.parseDouble(datas[x++]));
    quote.setHighPrice(Double.parseDouble(datas[x++]));
    quote.setLowPrice(Double.parseDouble(datas[x++]));
    quote.setClosePrice(Double.parseDouble(datas[x++]));
    quote.setVolume(Long.parseLong(datas[x++]));
    quote.setValue(Long.parseLong(datas[x++]));
    quote.setFreq(Long.parseLong(datas[x++]));
    quote.setPrevPrice(Double.parseDouble(datas[x++]));
    return quote;
  }

  /**
   * Converts a string to a Trade object.
   *
   * @param data the string data received from the broker.
   * @return a Trade object with parsed data.
   */
  public static Trade convertToTrade(String data) {
    String[] datas = StringUtils.split(data, "|");
    int x = 2;
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
   * Converts a string to an Index object.
   *
   * @param data the string data received from the broker.
   * @return an Index object with parsed data.
   */
  public static Index convertToIndex(String data) {
    String[] datas = StringUtils.split(data, "|");
    int x = 2;
    Index index = new Index();
    index.setIndexCode(datas[x++]);
    index.setDate(datas[x++]);
    index.setTime(datas[x++]);
    index.setLastIndex(Double.parseDouble(datas[x++]));
    index.setPrevIndex(Double.parseDouble(datas[x++]));
    index.setOpenIndex(Double.parseDouble(datas[x++]));
    index.setHighIndex(Double.parseDouble(datas[x++]));
    index.setLowIndex(Double.parseDouble(datas[x++]));
    index.setChange(Double.parseDouble(datas[x++]));
    index.setChangeRate(Double.parseDouble(datas[x++]));
    index.setFreq(Long.parseLong(datas[x++]));
    index.setVolume(Long.parseLong(datas[x++]));
    index.setValue(Double.parseDouble(datas[x++]));
    index.setUp(Integer.parseInt(datas[x++]));
    index.setDown(Integer.parseInt(datas[x++]));
    index.setUnchange(Integer.parseInt(datas[x++]));
    index.setNoTransaction(Integer.parseInt(datas[x++]));
    index.setMarketValue(Double.parseDouble(datas[x++]));
    return index;
  }

  /**
   * Converts a string to a Stock object.
   *
   * @param data the string data received from the broker.
   * @return a Stock object with parsed data.
   */
  public static Stock convertToStockData(String data) {
    String[] datas = StringUtils.split(data, "|");
    int x = 2;
    Stock stock = new Stock();
    stock.setSecurityCode(datas[x++]);
    stock.setSecurityName(datas[x++]);
    stock.setSecurityStatus(datas[x++]);
    stock.setSecurityType(datas[x++]);
    stock.setSubSector(datas[x++]);
    stock.setIpoPrice(Double.parseDouble(datas[x++]));
    stock.setBasePrice(Double.parseDouble(datas[x++]));
    stock.setListedShares(Integer.parseInt(datas[x++]));
    stock.setTradableListedShares(Integer.parseInt(datas[x++]));
    stock.setRemarks(datas[x++]);
    stock.setSecurityRemarks2(datas[x++]);
    stock.setWeight(Double.parseDouble(datas[x++]));
    return stock;
  }

  /**
   * Converts a string to a Broker object.
   *
   * @param data the string data received from the broker.
   * @return a Broker object with parsed data.
   */
  public static Broker convertToBrokerData(String data) {
    String[] datas = StringUtils.split(data, "|");
    int x = 2;
    Broker broker = new Broker();
    broker.setBrokerCode(datas[x++]);
    broker.setBrokerName(datas[x++]);
    broker.setBrokerStatus(datas[x++]);
    return broker;
  }

  /**
   * Converts a string to an OrderBook object representing the best bid.
   *
   * @param data the string data received from the broker.
   * @return an OrderBook object with the best bid data.
   */
  public static OrderBook convertToOrderBookBestBid(String data) {
    String[] datas = StringUtils.split(data, "|");
    int x = 2;
    OrderBook bestBid = new OrderBook();
    bestBid.setStockCode(datas[x++]);
    bestBid.setBidData(StringUtils.substring(data, 15));
    return bestBid;
  }

  /**
   * Converts a string to an OrderBook object representing the best offer.
   *
   * @param data the string data received from the broker.
   * @return an OrderBook object with the best offer data.
   */
  public static OrderBook convertToOrderBookBestOffer(String data) {
    String[] datas = StringUtils.split(data, "|");
    int x = 2;
    OrderBook bestOffer = new OrderBook();
    bestOffer.setStockCode(datas[x++]);
    bestOffer.setOfferData(StringUtils.substring(data, 15));
    return bestOffer;
  }
}
