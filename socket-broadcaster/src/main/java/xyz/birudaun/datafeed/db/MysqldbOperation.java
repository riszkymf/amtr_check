package xyz.birudaun.datafeed.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import xyz.birudaun.datafeed.beans.IntradayStockSummary;
import xyz.birudaun.datafeed.utils.CommonUtils;

public class MysqldbOperation {
  private static final String SQL_getTimeWithoutStockSummary = "SELECT temp.trade_time FROM (SELECT DISTINCT SUBSTRING(trade_time, 1, 4) AS trade_time FROM idx_trade WHERE board_code = 'RG' AND trade_date = CURRENT_DATE) AS temp \tLEFT JOIN (SELECT DISTINCT trade_time FROM intraday_stock_summary WHERE board_code = 'RG' AND trade_date = CURRENT_DATE) ss ON temp.trade_time = ss.trade_time WHERE ss.trade_time IS NULL";
  
  private static final String SQL_insertIntradayStockSummary = "INSERT INTO intraday_stock_summary (trade_date, trade_time, security_code, board_code, opening_price, highest_price, \tlowest_price, closing_price, traded_volume, traded_value, traded_frequency) \tSELECT trade_date, trade_time, security_code, board_code, \t\tSUM(opening_price) AS opening_price, SUM(highest_price) AS highest_price, SUM(lowest_price) AS lowest_price, \t\tSUM(closing_price) AS closing_price, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \t\tSUM(traded_frequency) AS traded_frequency \tFROM ( \t\tSELECT trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, security_code, board_code, \t\t\t0 AS opening_price, MAX(price) AS highest_price, MIN(price) AS lowest_price, 0 AS closing_price, \t\t\tSUM(volume) AS traded_volume, SUM(volume*price) AS traded_value, COUNT(*) AS traded_frequency \t\tFROM idx_trade \t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = CURRENT_DATE \t\tGROUP BY trade_date, SUBSTRING(trade_time, 1, 4), security_code, BOARD_CODE  \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\ttrd.price AS opening_price, 0 AS highest_price, 0 AS lowest_price, 0 AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM idx_trade trd, \t\t\t(SELECT MIN(CAST(TRADE_NUMBER AS UNSIGNED)) AS trade_number \t\t\tFROM idx_trade \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = CURRENT_DATE \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) opening_trade \t\tWHERE trd.trade_number = opening_trade.trade_number \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\t0 AS opening_price, 0 AS highest_price, 0 AS lowest_price, trd.price AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM idx_trade trd, \t\t\t(SELECT MAX(CAST(TRADE_NUMBER AS UNSIGNED)) AS trade_number \t\t\tFROM idx_trade \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = CURRENT_DATE \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) closing_trade \t\tWHERE trd.trade_number = closing_trade.trade_number \t) AS temp \tGROUP BY trade_date, trade_time, security_code, board_code ";
  
  private static final String SQL_calculateIntradayStockSummary = "\tSELECT trade_date, trade_time, security_code, board_code, \t\tSUM(opening_price) AS opening_price, SUM(highest_price) AS highest_price, SUM(lowest_price) AS lowest_price, \t\tSUM(closing_price) AS closing_price, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \t\tSUM(traded_frequency) AS traded_frequency \tFROM ( \t\tSELECT trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, security_code, board_code, \t\t\t0 AS opening_price, MAX(price) AS highest_price, MIN(price) AS lowest_price, 0 AS closing_price, \t\t\tSUM(volume) AS traded_volume, SUM(volume*price) AS traded_value, COUNT(*) AS traded_frequency \t\tFROM idx_trade \t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\tGROUP BY trade_date, SUBSTRING(trade_time, 1, 4), security_code, BOARD_CODE  \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\ttrd.price AS opening_price, 0 AS highest_price, 0 AS lowest_price, 0 AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM idx_trade trd, \t\t\t(SELECT MIN(CAST(TRADE_NUMBER AS UNSIGNED)) AS trade_number \t\t\tFROM idx_trade \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) opening_trade \t\tWHERE trd.trade_number = opening_trade.trade_number \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\t0 AS opening_price, 0 AS highest_price, 0 AS lowest_price, trd.price AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM idx_trade trd, \t\t\t(SELECT MAX(CAST(TRADE_NUMBER AS UNSIGNED)) AS trade_number \t\t\tFROM idx_trade \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) closing_trade \t\tWHERE trd.trade_number = closing_trade.trade_number \t) AS temp \tGROUP BY trade_date, trade_time, security_code, board_code ";
  
  private static final String SQL_getIntradayStockSummary = "SELECT * FROM intraday_stock_summary WHERE trade_time = ?";
  
  public static List<String> getTimeWithoutStockSummary() {
    Connection conn = null;
    PreparedStatement ps = null;
    List<String> listTime = new ArrayList<>();
    try {
      conn = DbConnection.getConnection();
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT temp.trade_time FROM (SELECT DISTINCT SUBSTRING(trade_time, 1, 4) AS trade_time FROM idx_trade WHERE board_code = 'RG' AND trade_date = CURRENT_DATE) AS temp \tLEFT JOIN (SELECT DISTINCT trade_time FROM intraday_stock_summary WHERE board_code = 'RG' AND trade_date = CURRENT_DATE) ss ON temp.trade_time = ss.trade_time WHERE ss.trade_time IS NULL");
      while (rs.next())
        listTime.add(rs.getString("trade_time")); 
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      try {
        if (ps != null)
          ps.close(); 
      } catch (SQLException e) {
        e.printStackTrace();
      } 
    } 
    return listTime;
  }
  
  public static void insertIntradayStockSummary(String time) {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      conn = DbConnection.getConnection();
      ps = conn.prepareStatement("INSERT INTO intraday_stock_summary (trade_date, trade_time, security_code, board_code, opening_price, highest_price, \tlowest_price, closing_price, traded_volume, traded_value, traded_frequency) \tSELECT trade_date, trade_time, security_code, board_code, \t\tSUM(opening_price) AS opening_price, SUM(highest_price) AS highest_price, SUM(lowest_price) AS lowest_price, \t\tSUM(closing_price) AS closing_price, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \t\tSUM(traded_frequency) AS traded_frequency \tFROM ( \t\tSELECT trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, security_code, board_code, \t\t\t0 AS opening_price, MAX(price) AS highest_price, MIN(price) AS lowest_price, 0 AS closing_price, \t\t\tSUM(volume) AS traded_volume, SUM(volume*price) AS traded_value, COUNT(*) AS traded_frequency \t\tFROM idx_trade \t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = CURRENT_DATE \t\tGROUP BY trade_date, SUBSTRING(trade_time, 1, 4), security_code, BOARD_CODE  \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\ttrd.price AS opening_price, 0 AS highest_price, 0 AS lowest_price, 0 AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM idx_trade trd, \t\t\t(SELECT MIN(CAST(TRADE_NUMBER AS UNSIGNED)) AS trade_number \t\t\tFROM idx_trade \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = CURRENT_DATE \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) opening_trade \t\tWHERE trd.trade_number = opening_trade.trade_number \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\t0 AS opening_price, 0 AS highest_price, 0 AS lowest_price, trd.price AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM idx_trade trd, \t\t\t(SELECT MAX(CAST(TRADE_NUMBER AS UNSIGNED)) AS trade_number \t\t\tFROM idx_trade \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = CURRENT_DATE \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) closing_trade \t\tWHERE trd.trade_number = closing_trade.trade_number \t) AS temp \tGROUP BY trade_date, trade_time, security_code, board_code ");
      ps.setString(1, time);
      ps.setString(2, time);
      ps.setString(3, time);
      ps.executeUpdate();
    } catch (Exception ex) {
      System.out.println("ERROR insertTop8BrokerDataByDate");
      ex.printStackTrace();
    } finally {
      if (ps != null)
        try {
          ps.close();
        } catch (Exception ex) {
          ex.printStackTrace();
        }  
    } 
  }
  
  public static List<IntradayStockSummary> calculateIntradayStockSummary(String time) {
    Connection conn = null;
    PreparedStatement ps = null;
    List<IntradayStockSummary> ssList = new ArrayList<>();
    try {
      conn = DbConnection.getConnection();
      ps = conn.prepareStatement("\tSELECT trade_date, trade_time, security_code, board_code, \t\tSUM(opening_price) AS opening_price, SUM(highest_price) AS highest_price, SUM(lowest_price) AS lowest_price, \t\tSUM(closing_price) AS closing_price, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \t\tSUM(traded_frequency) AS traded_frequency \tFROM ( \t\tSELECT trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, security_code, board_code, \t\t\t0 AS opening_price, MAX(price) AS highest_price, MIN(price) AS lowest_price, 0 AS closing_price, \t\t\tSUM(volume) AS traded_volume, SUM(volume*price) AS traded_value, COUNT(*) AS traded_frequency \t\tFROM idx_trade \t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\tGROUP BY trade_date, SUBSTRING(trade_time, 1, 4), security_code, BOARD_CODE  \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\ttrd.price AS opening_price, 0 AS highest_price, 0 AS lowest_price, 0 AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM idx_trade trd, \t\t\t(SELECT MIN(CAST(TRADE_NUMBER AS UNSIGNED)) AS trade_number \t\t\tFROM idx_trade \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) opening_trade \t\tWHERE trd.trade_number = opening_trade.trade_number \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\t0 AS opening_price, 0 AS highest_price, 0 AS lowest_price, trd.price AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM idx_trade trd, \t\t\t(SELECT MAX(CAST(TRADE_NUMBER AS UNSIGNED)) AS trade_number \t\t\tFROM idx_trade \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) closing_trade \t\tWHERE trd.trade_number = closing_trade.trade_number \t) AS temp \tGROUP BY trade_date, trade_time, security_code, board_code ");
      ps.setString(1, time);
      ps.setString(2, time);
      ps.setString(3, time);
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        IntradayStockSummary iss = new IntradayStockSummary();
        iss.setTradeDate(rs.getString("trade_date"));
        iss.setTradeTime(rs.getString("trade_time"));
        iss.setSecurityCode(rs.getString("security_code"));
        iss.setBoardCode(rs.getString("board_code"));
        iss.setOpeningPrice(rs.getDouble("opening_price"));
        iss.setHighestPrice(rs.getDouble("highest_price"));
        iss.setLowestPrice(rs.getDouble("lowest_price"));
        iss.setClosingPrice(rs.getDouble("closing_price"));
        iss.setTradedVolume(rs.getLong("traded_volume"));
        iss.setTradedValue(rs.getDouble("traded_value"));
        iss.setTradedFrequency(rs.getLong("traded_frequency"));
        ssList.add(iss);
      } 
    } catch (Exception ex) {
      System.out.println("ERROR insertTop8BrokerDataByDate");
      ex.printStackTrace();
    } finally {
      if (ps != null)
        try {
          ps.close();
        } catch (Exception ex) {
          ex.printStackTrace();
        }  
    } 
    return ssList;
  }
  
  public static List<String> getIntradayStockSummary(String time) {
    Connection conn = null;
    PreparedStatement ps = null;
    List<String> stockSummaries = new ArrayList<>();
    try {
      conn = DbConnection.getConnection();
      ps = conn.prepareStatement("SELECT * FROM intraday_stock_summary WHERE trade_time = ?");
      ps.setString(1, time);
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        String stockSummary = 
          String.valueOf(rs.getString("security_code")) + 
          "," + CommonUtils.getDateWithSeparator(rs.getString("trade_date"), "/") + 
          "," + CommonUtils.getTimeWithSeparator(rs.getString("trade_time"), ":") + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("opening_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("highest_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("lowest_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("closing_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("traded_volume")) + 
          ",0,0,0";
        stockSummaries.add(stockSummary);
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } finally {
      if (ps != null)
        try {
          ps.close();
        } catch (Exception ex) {
          ex.printStackTrace();
        }  
    } 
    return stockSummaries;
  }
}
