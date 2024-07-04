package dev.birudaun.antr.db;

import dev.birudaun.antr.beans.StockQuote;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresOperation {
  private static final String SQL_insertQuote = "INSERT INTO itch_stock_quote (stock_code, board, data_date, data_time, prev_price, prev_chg, prev_chg_rate, last_price, change_price, change_rate, open_price, high_price, low_price, avg_price, freq, volume, value, market_cap, best_bid_price, best_bid_volume, best_offer_price, best_offer_volume) VALUES (?, ?, CURRENT_DATE, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
  
  private static final String SQL_updateQuote = "UPDATE itch_stock_quote SET data_time = ?, prev_price = ?, prev_chg = ?, prev_chg_rate = ?, last_price = ?, change_price = ?, change_rate = ?, open_price = ?, high_price = ?, low_price = ?, avg_price = ?, freq = ?, volume = ?, value = ?, market_cap = ?, best_bid_price = ?, best_bid_volume = ?, best_offer_price = ?, best_offer_volume = ? WHERE stock_code = ? AND board = ? AND data_date = CURRENT_DATE";
  
  public static void saveStockQuote(StockQuote quote) {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      conn = PostgresConnection.getConnection();
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
}
