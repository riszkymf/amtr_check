package dev.birudaun.antr.db;

import dev.birudaun.antr.beans.Index;
import dev.birudaun.antr.beans.OrderBook;
import dev.birudaun.antr.beans.StockQuote;
import dev.birudaun.antr.beans.Trade;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.dbcp2.BasicDataSource;

public class HsqldbOperation {
  private static final String SQL_CREATE_TABLE_itch_stock_quote = "CREATE TABLE IF NOT EXISTS itch_stock_quote (   stock_code VARCHAR(21),   board VARCHAR(4),   data_date DATE,   data_time INT,   prev_price DECIMAL(14,2),   prev_chg DECIMAL(14,2),   prev_chg_rate DECIMAL(14,2),   last_price DECIMAL(14,2),   change_price DECIMAL(14,2),   change_rate DECIMAL(14,2),   open_price DECIMAL(14,2),   high_price DECIMAL(14,2),   low_price DECIMAL(14,2),   avg_price DECIMAL(14,2),   freq INT,   volume BIGINT,   value BIGINT,   market_cap BIGINT,   best_bid_price DECIMAL(14,2),   best_bid_volume INT,   best_offer_price DECIMAL(14,2),   best_offer_volume INT ) ";
  
  private static final String SQL_CREATE_INDEX_itch_stock_quote = "CREATE INDEX IF NOT EXISTS uk_itch_stock_quote ON itch_stock_quote (   stock_code, board, data_date )";
  
  private static final String SQL_CREATE_TABLE_itch_index = "CREATE TABLE IF NOT EXISTS itch_index (   index_code VARCHAR(20),   data_date DATE,   data_time INT,   last_index DECIMAL(12,4),   prev_index DECIMAL(12,4),   open_index DECIMAL(12,4),   high_index DECIMAL(12,4),   low_index DECIMAL(12,4),   change_index DECIMAL(12,4),   change_rate DECIMAL(12,4),   freq INT,   volume BIGINT,   value BIGINT,   up INT,   down INT,   unchange INT,   no_transaction INT,   base_value DECIMAL(14,2),   market_value BIGINT,   fg_buy_freq INT,   fg_sell_freq INT,   fg_buy_volume INT,   fg_sell_volume INT,   fg_buy_value DECIMAL(14,2),   fg_sell_value DECIMAL(14,2) )";
  
  private static final String SQL_CREATE_INDEX_itch_index = "CREATE INDEX IF NOT EXISTS uk_itch_index ON itch_index (   index_code, data_date )";
  
  private static final String SQL_CREATE_TABLE_itch_orderbook = "CREATE TABLE IF NOT EXISTS itch_orderbook (   stock_code VARCHAR(21),   board VARCHAR(4),   data_date DATE,   data_time INT,   bid_data VARCHAR(1000),   offer_data VARCHAR(1000) )";
  
  private static final String SQL_CREATE_INDEX_itch_orderbook = "CREATE INDEX IF NOT EXISTS uk_itch_orderbook ON itch_orderbook (   stock_code, board, data_date )";
  
  private static final String SQL_CREATE_TABLE_idx_trade = "CREATE TABLE IF NOT EXISTS idx_trade (   trade_date VARCHAR(8),   trade_time VARCHAR(6),   trade_command INT,   security_code VARCHAR(21),   board_code VARCHAR(4),   trade_number VARCHAR(12),   price DECIMAL(14,2),   volume BIGINT,   buyer_code VARCHAR(4),   buyer_type CHAR(1),   seller_code VARCHAR(4),   seller_type CHAR(1),   buyer_order_number VARCHAR(12),   seller_order_number VARCHAR(12),   transaction_type VARCHAR(5) )";
  
  private static final String SQL_CREATE_INDEX_idx_trade = "CREATE INDEX IF NOT EXISTS uk_idx_trade ON idx_trade (   trade_number, trade_date )";
  
  private static final String SQL_insertTrade = "INSERT INTO idx_trade (trade_date, trade_time, security_code, board_code, trade_number, price, volume) VALUES (?, ?, ?, ?, ?, ?, ?)";
  
  private static final String SQL_insertQuote = "INSERT INTO itch_stock_quote (stock_code, board, data_date, data_time, prev_price, prev_chg, prev_chg_rate, last_price, change_price, change_rate, open_price, high_price, low_price, avg_price, freq, volume, value, market_cap, best_bid_price, best_bid_volume, best_offer_price, best_offer_volume) VALUES (?, ?, CURRENT_DATE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  
  private static final String SQL_updateQuote = "UPDATE itch_stock_quote SET data_time = ?, prev_price = ?, prev_chg = ?, prev_chg_rate = ?, last_price = ?, change_price = ?, change_rate = ?, open_price = ?, high_price = ?, low_price = ?, avg_price = ?, freq = ?, volume = ?, value = ?, market_cap = ?, best_bid_price = ?, best_bid_volume = ?, best_offer_price = ?, best_offer_volume = ? WHERE stock_code = ? AND board = ? AND data_date = CURRENT_DATE";
  
  private static final String SQL_insertBestBid = "INSERT INTO itch_orderbook (stock_code, board, data_date, data_time, bid_data) VALUES (?, ?, CURRENT_DATE, ?, ?)";
  
  private static final String SQL_updateBestBid = "UPDATE itch_orderbook SET bid_data = ?, data_time = ? WHERE stock_code = ? AND board = ? AND data_date = CURRENT_DATE";
  
  private static final String SQL_insertOrderBook = "INSERT INTO itch_orderbook (stock_code, board, data_date, offer_data) VALUES (?, ?, CURRENT_DATE, ?)";
  
  private static final String SQL_updateOrderBook = "UPDATE itch_orderbook SET offer_data = ? WHERE stock_code = ? AND board = ? AND data_date = CURRENT_DATE";
  
  private static final String SQL_insertIndex = "INSERT INTO itch_index (index_code, data_date, data_time, last_index, prev_index, open_index, high_index, low_index, change_index, change_rate, freq, volume, value, up, down, unchange, no_transaction, base_value, market_value) VALUES (?, CURRENT_DATE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  
  private static final String SQL_updateIndex = "UPDATE itch_index SET data_time = ?, last_index = ?, prev_index = ?, open_index = ?, high_index = ?, low_index = ?, change_index = ?, change_rate = ?, freq = ?, volume = ?, value = ?, up = ?, down = ?, unchange = ?, no_transaction = ?, base_value = ?, market_value = ? WHERE index_code = ? AND data_date = CURRENT_DATE";
  
  public static void createTables() {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      BasicDataSource dataSource = HsqldbDataSource.getInstance().getDataSource();
      conn = dataSource.getConnection();
      ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS itch_stock_quote (   stock_code VARCHAR(21),   board VARCHAR(4),   data_date DATE,   data_time INT,   prev_price DECIMAL(14,2),   prev_chg DECIMAL(14,2),   prev_chg_rate DECIMAL(14,2),   last_price DECIMAL(14,2),   change_price DECIMAL(14,2),   change_rate DECIMAL(14,2),   open_price DECIMAL(14,2),   high_price DECIMAL(14,2),   low_price DECIMAL(14,2),   avg_price DECIMAL(14,2),   freq INT,   volume BIGINT,   value BIGINT,   market_cap BIGINT,   best_bid_price DECIMAL(14,2),   best_bid_volume INT,   best_offer_price DECIMAL(14,2),   best_offer_volume INT ) ");
      int row = ps.executeUpdate();
      ps = conn.prepareStatement("CREATE INDEX IF NOT EXISTS uk_itch_stock_quote ON itch_stock_quote (   stock_code, board, data_date )");
      ps.executeUpdate();
      System.out.println("TABLE itch_stock_quote CREATED...");
      ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS itch_index (   index_code VARCHAR(20),   data_date DATE,   data_time INT,   last_index DECIMAL(12,4),   prev_index DECIMAL(12,4),   open_index DECIMAL(12,4),   high_index DECIMAL(12,4),   low_index DECIMAL(12,4),   change_index DECIMAL(12,4),   change_rate DECIMAL(12,4),   freq INT,   volume BIGINT,   value BIGINT,   up INT,   down INT,   unchange INT,   no_transaction INT,   base_value DECIMAL(14,2),   market_value BIGINT,   fg_buy_freq INT,   fg_sell_freq INT,   fg_buy_volume INT,   fg_sell_volume INT,   fg_buy_value DECIMAL(14,2),   fg_sell_value DECIMAL(14,2) )");
      ps.executeUpdate();
      ps = conn.prepareStatement("CREATE INDEX IF NOT EXISTS uk_itch_index ON itch_index (   index_code, data_date )");
      ps.executeUpdate();
      System.out.println("TABLE itch_index CREATED...");
      ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS itch_orderbook (   stock_code VARCHAR(21),   board VARCHAR(4),   data_date DATE,   data_time INT,   bid_data VARCHAR(1000),   offer_data VARCHAR(1000) )");
      ps.executeUpdate();
      ps = conn.prepareStatement("CREATE INDEX IF NOT EXISTS uk_itch_orderbook ON itch_orderbook (   stock_code, board, data_date )");
      ps.executeUpdate();
      System.out.println("TABLE itch_orderbook CREATED...");
      ps = conn.prepareStatement("CREATE TABLE IF NOT EXISTS idx_trade (   trade_date VARCHAR(8),   trade_time VARCHAR(6),   trade_command INT,   security_code VARCHAR(21),   board_code VARCHAR(4),   trade_number VARCHAR(12),   price DECIMAL(14,2),   volume BIGINT,   buyer_code VARCHAR(4),   buyer_type CHAR(1),   seller_code VARCHAR(4),   seller_type CHAR(1),   buyer_order_number VARCHAR(12),   seller_order_number VARCHAR(12),   transaction_type VARCHAR(5) )");
      ps.executeUpdate();
      ps = conn.prepareStatement("CREATE INDEX IF NOT EXISTS uk_idx_trade ON idx_trade (   trade_number, trade_date )");
      ps.executeUpdate();
      System.out.println("TABLE idx_trade CREATED...");
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      try {
        if (ps != null)
          ps.close(); 
        if (conn != null)
          conn.close(); 
      } catch (SQLException e) {
        e.printStackTrace();
      } 
    } 
  }
  
  public static void saveTrade(Trade trade) {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      BasicDataSource dataSource = HsqldbDataSource.getInstance().getDataSource();
      conn = dataSource.getConnection();
      ps = conn.prepareStatement("INSERT INTO idx_trade (trade_date, trade_time, security_code, board_code, trade_number, price, volume) VALUES (?, ?, ?, ?, ?, ?, ?)");
      ps.setString(1, trade.getTradeDate());
      ps.setString(2, trade.getTradeTime());
      ps.setString(3, trade.getSecurityCode());
      ps.setString(4, trade.getBoardCode());
      ps.setString(5, trade.getTradeNo());
      ps.setDouble(6, trade.getPrice());
      ps.setLong(7, trade.getVolume());
      ps.executeUpdate();
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      try {
        if (ps != null)
          ps.close(); 
        if (conn != null)
          conn.close(); 
      } catch (SQLException e) {
        e.printStackTrace();
      } 
    } 
  }
  
  public static void saveStockQuote(StockQuote quote) {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      BasicDataSource dataSource = HsqldbDataSource.getInstance().getDataSource();
      conn = dataSource.getConnection();
      ps = conn.prepareStatement("UPDATE itch_stock_quote SET data_time = ?, prev_price = ?, prev_chg = ?, prev_chg_rate = ?, last_price = ?, change_price = ?, change_rate = ?, open_price = ?, high_price = ?, low_price = ?, avg_price = ?, freq = ?, volume = ?, value = ?, market_cap = ?, best_bid_price = ?, best_bid_volume = ?, best_offer_price = ?, best_offer_volume = ? WHERE stock_code = ? AND board = ? AND data_date = CURRENT_DATE");
      ps.setString(1, quote.getTime());
      ps.setDouble(2, quote.getPrevPrice());
      ps.setDouble(3, quote.getPrevChg());
      ps.setDouble(4, quote.getPrevChgRate());
      ps.setDouble(5, quote.getClosePrice());
      ps.setDouble(6, quote.getChange());
      ps.setDouble(7, quote.getChangeRate());
      ps.setDouble(8, quote.getOpenPrice());
      ps.setDouble(9, quote.getHighPrice());
      ps.setDouble(10, quote.getLowPrice());
      ps.setDouble(11, quote.getAvgPrice());
      ps.setLong(12, quote.getFreq());
      ps.setLong(13, quote.getVolume());
      ps.setDouble(14, quote.getValue());
      ps.setDouble(15, quote.getMarketCap());
      ps.setDouble(16, quote.getBestBidPrice());
      ps.setLong(17, quote.getBestBidVolume());
      ps.setDouble(18, quote.getBestOfferPrice());
      ps.setLong(19, quote.getBestOfferVolume());
      ps.setString(20, quote.getSecurityCode());
      ps.setString(21, quote.getBoardCode());
      int row = ps.executeUpdate();
      if (row == 0) {
        ps = conn.prepareStatement("INSERT INTO itch_stock_quote (stock_code, board, data_date, data_time, prev_price, prev_chg, prev_chg_rate, last_price, change_price, change_rate, open_price, high_price, low_price, avg_price, freq, volume, value, market_cap, best_bid_price, best_bid_volume, best_offer_price, best_offer_volume) VALUES (?, ?, CURRENT_DATE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        ps.setString(1, quote.getSecurityCode());
        ps.setString(2, quote.getBoardCode());
        ps.setString(3, quote.getTime());
        ps.setDouble(4, quote.getPrevPrice());
        ps.setDouble(5, quote.getPrevChg());
        ps.setDouble(6, quote.getPrevChgRate());
        ps.setDouble(7, quote.getClosePrice());
        ps.setDouble(8, quote.getChange());
        ps.setDouble(9, quote.getChangeRate());
        ps.setDouble(10, quote.getOpenPrice());
        ps.setDouble(11, quote.getHighPrice());
        ps.setDouble(12, quote.getLowPrice());
        ps.setDouble(13, quote.getAvgPrice());
        ps.setLong(14, quote.getFreq());
        ps.setLong(15, quote.getVolume());
        ps.setDouble(16, quote.getValue());
        ps.setDouble(17, quote.getMarketCap());
        ps.setDouble(18, quote.getBestBidPrice());
        ps.setLong(19, quote.getBestBidVolume());
        ps.setDouble(20, quote.getBestOfferPrice());
        ps.setLong(21, quote.getBestOfferVolume());
        ps.executeUpdate();
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      try {
        if (ps != null)
          ps.close(); 
        if (conn != null)
          conn.close(); 
      } catch (SQLException e) {
        e.printStackTrace();
      } 
    } 
  }
  
  public static void saveBestBid(OrderBook bestBid) {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      String time = (new SimpleDateFormat("HHmm")).format(new Date());
      int itime = Integer.parseInt(time);
      BasicDataSource dataSource = HsqldbDataSource.getInstance().getDataSource();
      conn = dataSource.getConnection();
      ps = conn.prepareStatement("UPDATE itch_orderbook SET bid_data = ?, data_time = ? WHERE stock_code = ? AND board = ? AND data_date = CURRENT_DATE");
      ps.setString(1, bestBid.getBidData());
      ps.setInt(2, itime);
      ps.setString(3, bestBid.getStockCode());
      ps.setString(4, bestBid.getBoard());
      int row = ps.executeUpdate();
      if (row == 0) {
        ps = conn.prepareStatement("INSERT INTO itch_orderbook (stock_code, board, data_date, data_time, bid_data) VALUES (?, ?, CURRENT_DATE, ?, ?)");
        ps.setString(1, bestBid.getStockCode());
        ps.setString(2, bestBid.getBoard());
        ps.setInt(3, itime);
        ps.setString(4, bestBid.getBidData());
        ps.executeUpdate();
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      try {
        if (ps != null)
          ps.close(); 
        if (conn != null)
          conn.close(); 
      } catch (SQLException e) {
        e.printStackTrace();
      } 
    } 
  }
  
  public static void saveBestOffer(OrderBook bestOffer) {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      BasicDataSource dataSource = HsqldbDataSource.getInstance().getDataSource();
      conn = dataSource.getConnection();
      ps = conn.prepareStatement("UPDATE itch_orderbook SET offer_data = ? WHERE stock_code = ? AND board = ? AND data_date = CURRENT_DATE");
      ps.setString(1, bestOffer.getOfferData());
      ps.setString(2, bestOffer.getStockCode());
      ps.setString(3, bestOffer.getBoard());
      int row = ps.executeUpdate();
      if (row == 0) {
        ps = conn.prepareStatement("INSERT INTO itch_orderbook (stock_code, board, data_date, offer_data) VALUES (?, ?, CURRENT_DATE, ?)");
        ps.setString(1, bestOffer.getStockCode());
        ps.setString(2, bestOffer.getBoard());
        ps.setString(3, bestOffer.getOfferData());
        ps.executeUpdate();
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      try {
        if (ps != null)
          ps.close(); 
        if (conn != null)
          conn.close(); 
      } catch (SQLException e) {
        e.printStackTrace();
      } 
    } 
  }
  
  public static void saveIndex(Index index) {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      BasicDataSource dataSource = HsqldbDataSource.getInstance().getDataSource();
      conn = dataSource.getConnection();
      ps = conn.prepareStatement("UPDATE itch_index SET data_time = ?, last_index = ?, prev_index = ?, open_index = ?, high_index = ?, low_index = ?, change_index = ?, change_rate = ?, freq = ?, volume = ?, value = ?, up = ?, down = ?, unchange = ?, no_transaction = ?, base_value = ?, market_value = ? WHERE index_code = ? AND data_date = CURRENT_DATE");
      ps.setString(1, index.getTime());
      ps.setDouble(2, index.getLastIndex());
      ps.setDouble(3, index.getPrevIndex());
      ps.setDouble(4, index.getOpenIndex());
      ps.setDouble(5, index.getHighIndex());
      ps.setDouble(6, index.getLowIndex());
      ps.setDouble(7, index.getChange());
      ps.setDouble(8, index.getChangeRate());
      ps.setLong(9, index.getFreq());
      ps.setLong(10, index.getVolume());
      ps.setDouble(11, index.getValue());
      ps.setInt(12, index.getUp());
      ps.setInt(13, index.getDown());
      ps.setInt(14, index.getUnchange());
      ps.setInt(15, index.getNoTransaction());
      ps.setDouble(16, index.getBaseValue());
      ps.setDouble(17, index.getMarketValue());
      ps.setString(18, index.getIndexCode());
      int row = ps.executeUpdate();
      if (row == 0) {
        ps = conn.prepareStatement("INSERT INTO itch_index (index_code, data_date, data_time, last_index, prev_index, open_index, high_index, low_index, change_index, change_rate, freq, volume, value, up, down, unchange, no_transaction, base_value, market_value) VALUES (?, CURRENT_DATE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        ps.setString(1, index.getIndexCode());
        ps.setString(2, index.getTime());
        ps.setDouble(3, index.getLastIndex());
        ps.setDouble(4, index.getPrevIndex());
        ps.setDouble(5, index.getOpenIndex());
        ps.setDouble(6, index.getHighIndex());
        ps.setDouble(7, index.getLowIndex());
        ps.setDouble(8, index.getChange());
        ps.setDouble(9, index.getChangeRate());
        ps.setLong(10, index.getFreq());
        ps.setLong(11, index.getVolume());
        ps.setDouble(12, index.getValue());
        ps.setInt(13, index.getUp());
        ps.setInt(14, index.getDown());
        ps.setInt(15, index.getUnchange());
        ps.setInt(16, index.getNoTransaction());
        ps.setDouble(17, index.getBaseValue());
        ps.setDouble(18, index.getMarketValue());
        ps.executeUpdate();
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      try {
        if (ps != null)
          ps.close(); 
        if (conn != null)
          conn.close(); 
      } catch (SQLException e) {
        e.printStackTrace();
      } 
    } 
  }
}
