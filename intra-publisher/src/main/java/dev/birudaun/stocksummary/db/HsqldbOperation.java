package dev.birudaun.stocksummary.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.sql.SQLException;


public class HsqldbOperation {
  private static final DateFormat SDF = new SimpleDateFormat("yyyyMMdd");
  
  private static final String SQL_getIntradayStockSummary = "SELECT trade_date, trade_time, security_code, board_code, \tSUM(opening_price) AS opening_price, SUM(highest_price) AS highest_price, SUM(lowest_price) AS lowest_price, \tSUM(closing_price) AS closing_price, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \tSUM(traded_frequency) AS traded_frequency FROM ( \tSELECT trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, security_code, board_code, \t\t0 AS opening_price, MAX(price) AS highest_price, MIN(price) AS lowest_price, 0 AS closing_price, \t\tSUM(volume) AS traded_volume, SUM(volume*price) AS traded_value, COUNT(*) AS traded_frequency \tFROM IDX_TRADE \tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = ? \tGROUP BY trade_date, SUBSTRING(trade_time, 1, 4), security_code, BOARD_CODE  \tUNION ALL \tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\ttrd.price AS opening_price, 0 AS highest_price, 0 AS lowest_price, 0 AS closing_price, \t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \tFROM IDX_TRADE trd, \t\t(SELECT MIN(CAST(TRADE_NUMBER AS integer)) AS trade_number \t\tFROM IDX_TRADE \t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = ? \t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) opening_trade \tWHERE trd.trade_number = opening_trade.trade_number \tUNION ALL \tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t0 AS opening_price, 0 AS highest_price, 0 AS lowest_price, trd.price AS closing_price, \t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \tFROM IDX_TRADE trd, \t\t(SELECT MAX(CAST(TRADE_NUMBER AS integer)) AS trade_number \t\tFROM IDX_TRADE \t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = ? \t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) closing_trade \tWHERE trd.trade_number = closing_trade.trade_number ) AS temp GROUP BY trade_date, trade_time, security_code, board_code ORDER BY security_code ";

  /**
   * Retrieves intraday stock summary data from the IDX_TRADE table for a specified time and current date.
   * Aggregates data including opening, highest, lowest, and closing prices, traded volume, value, and frequency.
   *
   * @param time The time substring (HHMM) to query data for, e.g., "0930".
   * @return A list of strings where each string represents summarized stock data in the format:
   *         "trade_date|trade_time|security_code|board_code|opening_price|highest_price|lowest_price|closing_price|traded_volume|traded_value|traded_frequency".
   * @throws SQLException If a database access error occurs or SQL execution fails.
   */
  public static List<String> getIntradayStockSummary(String time) {
    Connection conn = null;
    PreparedStatement ps = null;
    String now = SDF.format(new Date());
    List<String> results = new ArrayList<>();
    try {
      conn = HsqldbConnection.getConnection();
      ps = conn.prepareStatement("SELECT trade_date, trade_time, security_code, board_code, \tSUM(opening_price) AS opening_price, SUM(highest_price) AS highest_price, SUM(lowest_price) AS lowest_price, \tSUM(closing_price) AS closing_price, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \tSUM(traded_frequency) AS traded_frequency FROM ( \tSELECT trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, security_code, board_code, \t\t0 AS opening_price, MAX(price) AS highest_price, MIN(price) AS lowest_price, 0 AS closing_price, \t\tSUM(volume) AS traded_volume, SUM(volume*price) AS traded_value, COUNT(*) AS traded_frequency \tFROM IDX_TRADE \tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = ? \tGROUP BY trade_date, SUBSTRING(trade_time, 1, 4), security_code, BOARD_CODE  \tUNION ALL \tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\ttrd.price AS opening_price, 0 AS highest_price, 0 AS lowest_price, 0 AS closing_price, \t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \tFROM IDX_TRADE trd, \t\t(SELECT MIN(CAST(TRADE_NUMBER AS integer)) AS trade_number \t\tFROM IDX_TRADE \t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = ? \t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) opening_trade \tWHERE trd.trade_number = opening_trade.trade_number \tUNION ALL \tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t0 AS opening_price, 0 AS highest_price, 0 AS lowest_price, trd.price AS closing_price, \t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \tFROM IDX_TRADE trd, \t\t(SELECT MAX(CAST(TRADE_NUMBER AS integer)) AS trade_number \t\tFROM IDX_TRADE \t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = ? \t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) closing_trade \tWHERE trd.trade_number = closing_trade.trade_number ) AS temp GROUP BY trade_date, trade_time, security_code, board_code ORDER BY security_code ");
      ps.setString(1, time);
      ps.setString(2, now);
      ps.setString(3, time);
      ps.setString(4, now);
      ps.setString(5, time);
      ps.setString(6, now);
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        String result = String.valueOf(rs.getString(1)) + "|" + rs.getString(2) + "|" + rs.getString(3) + "|" + rs.getString(4) + "|" + 
          rs.getString(5) + "|" + rs.getString(6) + "|" + rs.getString(7) + "|" + rs.getString(8) + "|" + 
          rs.getString(9) + "|" + rs.getString(10);
        results.add(result);
      } 
    } catch (Exception ex) {
      System.out.println("ERROR getIntradayStockSummary");
      ex.printStackTrace();
    } finally {
      if (ps != null)
        try {
          ps.close();
        } catch (Exception ex) {
          ex.printStackTrace();
        }  
    } 
    return results;
  }
}
