package dev.birudaun.stocksummary.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class MysqlOperation {
  private static final String SQL_getIntradayIndexSummary = "SELECT data_date, data_time, index_code, 'RG' AS board_code, \tSUM(opening_index) AS opening_index, SUM(highest_index) AS highest_index, SUM(lowest_index) AS lowest_index, \tSUM(closing_index) AS closing_index, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \tSUM(traded_frequency) AS traded_frequency FROM ( \tSELECT data_date, SUBSTRING(data_time, 1, 4) AS data_time, index_code, \t\t0 AS opening_index, MAX(last_index) AS highest_index, MIN(last_index) AS lowest_index, 0 AS closing_index, \t\t0 AS traded_volume, 0 AS traded_value, COUNT(*) AS traded_frequency \tFROM itch_index_intraday \tWHERE SUBSTRING(data_time, 1, 4) = ? AND data_date = CURRENT_DATE \tGROUP BY data_date, SUBSTRING(data_time, 1, 4), index_code \tUNION ALL \tSELECT trd.data_date, SUBSTRING(data_time, 1, 4) AS data_time, trd.index_code,  \t\ttrd.last_index AS opening_index, 0 AS highest_index, 0 AS lowest_index, 0 AS closing_index, \t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \tFROM itch_index_intraday trd, \t\t(SELECT index_code, MIN(id) AS index_number \t\tFROM itch_index_intraday \t\tWHERE SUBSTRING(data_time, 1, 4) = ? AND data_date = CURRENT_DATE \t\tGROUP BY SUBSTRING(data_time, 1, 4), index_code) opening_trade \tWHERE trd.id = opening_trade.index_number \tUNION ALL \tSELECT trd.data_date, SUBSTRING(data_time, 1, 4) AS data_time, trd.index_code, \t\t0 AS opening_index, 0 AS highest_index, 0 AS lowest_index, trd.last_index AS closing_index, \t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \tFROM itch_index_intraday trd, \t\t(SELECT MAX(CAST(id AS INTEGER)) AS index_number \t\tFROM itch_index_intraday \t\tWHERE SUBSTRING(data_time, 1, 4) = ? AND data_date = CURRENT_DATE \t\tGROUP BY SUBSTRING(data_time, 1, 4), index_code) closing_trade \tWHERE trd.id = closing_trade.index_number ) AS temp GROUP BY data_date, data_time, index_code ORDER BY index_code ";
  
  private static final DateFormat DF = new SimpleDateFormat("yyyyMMdd");
  
  public static List<String> getIntradayIndexSummary(String time) {
    System.out.println("\ttime: " + time);
    PreparedStatement ps = null;
    List<String> results = new ArrayList<>();
    try {
      Connection conn = MysqlConnection.getConnection();
      ps = conn.prepareStatement("SELECT data_date, data_time, index_code, 'RG' AS board_code, \tSUM(opening_index) AS opening_index, SUM(highest_index) AS highest_index, SUM(lowest_index) AS lowest_index, \tSUM(closing_index) AS closing_index, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \tSUM(traded_frequency) AS traded_frequency FROM ( \tSELECT data_date, SUBSTRING(data_time, 1, 4) AS data_time, index_code, \t\t0 AS opening_index, MAX(last_index) AS highest_index, MIN(last_index) AS lowest_index, 0 AS closing_index, \t\t0 AS traded_volume, 0 AS traded_value, COUNT(*) AS traded_frequency \tFROM itch_index_intraday \tWHERE SUBSTRING(data_time, 1, 4) = ? AND data_date = CURRENT_DATE \tGROUP BY data_date, SUBSTRING(data_time, 1, 4), index_code \tUNION ALL \tSELECT trd.data_date, SUBSTRING(data_time, 1, 4) AS data_time, trd.index_code,  \t\ttrd.last_index AS opening_index, 0 AS highest_index, 0 AS lowest_index, 0 AS closing_index, \t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \tFROM itch_index_intraday trd, \t\t(SELECT index_code, MIN(id) AS index_number \t\tFROM itch_index_intraday \t\tWHERE SUBSTRING(data_time, 1, 4) = ? AND data_date = CURRENT_DATE \t\tGROUP BY SUBSTRING(data_time, 1, 4), index_code) opening_trade \tWHERE trd.id = opening_trade.index_number \tUNION ALL \tSELECT trd.data_date, SUBSTRING(data_time, 1, 4) AS data_time, trd.index_code, \t\t0 AS opening_index, 0 AS highest_index, 0 AS lowest_index, trd.last_index AS closing_index, \t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \tFROM itch_index_intraday trd, \t\t(SELECT MAX(CAST(id AS INTEGER)) AS index_number \t\tFROM itch_index_intraday \t\tWHERE SUBSTRING(data_time, 1, 4) = ? AND data_date = CURRENT_DATE \t\tGROUP BY SUBSTRING(data_time, 1, 4), index_code) closing_trade \tWHERE trd.id = closing_trade.index_number ) AS temp GROUP BY data_date, data_time, index_code ORDER BY index_code ");
      ps.setString(1, time);
      ps.setString(2, time);
      ps.setString(3, time);
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        String date = DF.format(rs.getDate(1));
        String result = String.valueOf(date) + "|" + rs.getString(2) + "|" + rs.getString(3) + "|" + rs.getString(4) + "|" + 
          rs.getString(5) + "|" + rs.getString(6) + "|" + rs.getString(7) + "|" + rs.getString(8) + "|" + 
          rs.getString(9) + "|" + rs.getString(10);
        System.out.println("\t" + result);
        results.add(result);
      } 
    } catch (Exception ex) {
      System.out.println("ERROR getIntradayIndexSummary");
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
