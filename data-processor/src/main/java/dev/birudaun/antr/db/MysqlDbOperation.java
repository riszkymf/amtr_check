package dev.birudaun.antr.db;

import dev.birudaun.antr.beans.Broker;
import dev.birudaun.antr.beans.Index;
import dev.birudaun.antr.beans.Stock;
import dev.birudaun.antr.beans.StockQuote;
import dev.birudaun.antr.beans.Trade;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlDbOperation {
  public static void saveStockData(Stock stock) {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      conn = MysqlConnection.getConnection();
      ps = conn.prepareStatement("UPDATE idx_stock_data SET security_name = ?, security_status = ?, security_type = ?, sub_sector = ?, ipo_price = ?, base_price = ?, listed_shares = ?, tradable_listed_shares = ?, shares_per_lot = ?, remarks = ?, security_remarks2 = ?, weight = ? WHERE security_code = ?");
      ps.setString(1, stock.getSecurityName());
      ps.setString(2, stock.getSecurityStatus());
      ps.setString(3, stock.getSecurityType());
      ps.setString(4, stock.getSubSector());
      ps.setDouble(5, stock.getIpoPrice());
      ps.setDouble(6, stock.getBasePrice());
      ps.setLong(7, stock.getListedShares());
      ps.setLong(8, stock.getTradableListedShares());
      ps.setInt(9, stock.getSharesPerLot());
      ps.setString(10, stock.getRemarks());
      ps.setString(11, stock.getSecurityRemarks2());
      ps.setDouble(12, stock.getWeight());
      ps.setString(13, stock.getSecurityCode());
      int row = ps.executeUpdate();
      if (row == 0) {
        ps = conn.prepareStatement("INSERT INTO idx_stock_data (security_code, security_name, security_status, security_type, sub_sector, ipo_price, base_price, listed_shares, tradable_listed_shares, shares_per_lot, remarks, security_remarks2, weight) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        ps.setString(1, stock.getSecurityCode());
        ps.setString(2, stock.getSecurityName());
        ps.setString(3, stock.getSecurityStatus());
        ps.setString(4, stock.getSecurityType());
        ps.setString(5, stock.getSubSector());
        ps.setDouble(6, stock.getIpoPrice());
        ps.setDouble(7, stock.getBasePrice());
        ps.setLong(8, stock.getListedShares());
        ps.setLong(9, stock.getTradableListedShares());
        ps.setInt(10, stock.getSharesPerLot());
        ps.setString(11, stock.getRemarks());
        ps.setString(12, stock.getSecurityRemarks2());
        ps.setDouble(13, stock.getWeight());
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
  
  public static void saveBrokerData(Broker broker) {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      conn = MysqlConnection.getConnection();
      ps = conn.prepareStatement("UPDATE idx_broker_data SET broker_name = ?, broker_status = ? WHERE broker_code = ?");
      ps.setString(1, broker.getBrokerName());
      ps.setString(2, broker.getBrokerStatus());
      ps.setString(3, broker.getBrokerCode());
      int row = ps.executeUpdate();
      if (row == 0) {
        ps = conn.prepareStatement("INSERT INTO idx_broker_data (broker_code, broker_name, broker_status) VALUES (?, ?, ?)");
        ps.setString(1, broker.getBrokerCode());
        ps.setString(2, broker.getBrokerName());
        ps.setString(3, broker.getBrokerStatus());
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
      conn = MysqlConnection.getConnection();
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
  
  public static void saveStockQuote(StockQuote quote) {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      conn = MysqlConnection.getConnection();
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
  
  public static void saveTrade(Trade trade) {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      conn = MysqlConnection.getConnection();
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
}
