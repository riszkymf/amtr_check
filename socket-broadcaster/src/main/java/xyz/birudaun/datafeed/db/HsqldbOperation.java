package xyz.birudaun.datafeed.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import xyz.birudaun.datafeed.beans.IntradayStockSummary;
import xyz.birudaun.datafeed.utils.CommonUtils;

public class HsqldbOperation {
  private static final DateFormat SDF = new SimpleDateFormat("yyyyMMdd");
  
  private static final String SQL_CREATE_TABLE_intraday_stock_summary = "CREATE TABLE intraday_stock_summary (   trade_date VARCHAR(8),   trade_time VARCHAR(4),   security_code VARCHAR(21),   board_code VARCHAR(4),   opening_price DECIMAL(14,2),   highest_price DECIMAL(14,2),   lowest_price DECIMAL(14,2),   closing_price DECIMAL(14,2),   previous_price DECIMAL(14,2),   traded_volume BIGINT,   traded_value BIGINT,   traded_frequency INT )";
  
  private static final String SQL_CREATE_INDEX_intraday_stock_summary = "CREATE INDEX IF NOT EXISTS uk_intraday_stock_summary ON intraday_stock_summary   trade_date, trade_time, security_code, board_code )";
  
  private static final String SQL_getTimeWithoutStockSummary = "SELECT temp.trade_time FROM (SELECT DISTINCT SUBSTRING(trade_time, 1, 4) AS trade_time FROM idx_trade WHERE board_code = 'RG' AND trade_date = ?) AS temp \tLEFT JOIN (SELECT DISTINCT trade_time FROM intraday_stock_summary WHERE board_code = 'RG' AND trade_date = ?) ss ON temp.trade_time = ss.trade_time WHERE ss.trade_time IS NULL";
  
  private static final String SQL_insertIntradayStockSummary = "INSERT INTO PUBLIC.INTRADAY_STOCK_SUMMARY (trade_date, trade_time, security_code, board_code, opening_price, highest_price, \tlowest_price, closing_price, traded_volume, traded_value, traded_frequency) \tSELECT trade_date, trade_time, security_code, board_code, \t\tSUM(opening_price) AS opening_price, SUM(highest_price) AS highest_price, SUM(lowest_price) AS lowest_price, \t\tSUM(closing_price) AS closing_price, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \t\tSUM(traded_frequency) AS traded_frequency \tFROM ( \t\tSELECT trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, security_code, board_code, \t\t\t0 AS opening_price, MAX(price) AS highest_price, MIN(price) AS lowest_price, 0 AS closing_price, \t\t\tSUM(volume) AS traded_volume, SUM(volume*price) AS traded_value, COUNT(*) AS traded_frequency \t\tFROM IDX_TRADE \t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = ? \t\tGROUP BY trade_date, SUBSTRING(trade_time, 1, 4), security_code, BOARD_CODE  \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\ttrd.price AS opening_price, 0 AS highest_price, 0 AS lowest_price, 0 AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM IDX_TRADE trd, \t\t\t(SELECT MIN(CAST(TRADE_NUMBER AS integer)) AS trade_number \t\t\tFROM IDX_TRADE \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = ? \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) opening_trade \t\tWHERE trd.trade_number = opening_trade.trade_number \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\t0 AS opening_price, 0 AS highest_price, 0 AS lowest_price, trd.price AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM IDX_TRADE trd, \t\t\t(SELECT MAX(CAST(TRADE_NUMBER AS integer)) AS trade_number \t\t\tFROM IDX_TRADE \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = ? \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) closing_trade \t\tWHERE trd.trade_number = closing_trade.trade_number \t) AS temp \tGROUP BY trade_date, trade_time, security_code, board_code ";
  
  private static final String SQL_calculateIntradayStockSummary = "\tSELECT trade_date, trade_time, security_code, board_code, \t\tSUM(opening_price) AS opening_price, SUM(highest_price) AS highest_price, SUM(lowest_price) AS lowest_price, \t\tSUM(closing_price) AS closing_price, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \t\tSUM(traded_frequency) AS traded_frequency \tFROM ( \t\tSELECT trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, security_code, board_code, \t\t\t0 AS opening_price, MAX(price) AS highest_price, MIN(price) AS lowest_price, 0 AS closing_price, \t\t\tSUM(volume) AS traded_volume, SUM(volume*price) AS traded_value, COUNT(*) AS traded_frequency \t\tFROM IDX_TRADE \t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\tGROUP BY trade_date, SUBSTRING(trade_time, 1, 4), security_code, BOARD_CODE  \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\ttrd.price AS opening_price, 0 AS highest_price, 0 AS lowest_price, 0 AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM IDX_TRADE trd, \t\t\t(SELECT MIN(CAST(TRADE_NUMBER AS integer)) AS trade_number \t\t\tFROM IDX_TRADE \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) opening_trade \t\tWHERE trd.trade_number = opening_trade.trade_number \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\t0 AS opening_price, 0 AS highest_price, 0 AS lowest_price, trd.price AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM IDX_TRADE trd, \t\t\t(SELECT MAX(CAST(TRADE_NUMBER AS integer)) AS trade_number \t\t\tFROM IDX_TRADE \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) closing_trade \t\tWHERE trd.trade_number = closing_trade.trade_number \t) AS temp \tGROUP BY trade_date, trade_time, security_code, board_code ";
  
  private static final String SQL_getTodayIntradayStockSummary = "SELECT * FROM intraday_stock_summary WHERE TRADE_DATE = ?";
  
  private static final String SQL_getIntradayStockSummary = "SELECT * FROM intraday_stock_summary WHERE trade_time = ? AND trade_date = ? ";
  
  private static final String SQL_CHECKPOINT_DEFRAG = "CHECKPOINT DEFRAG";
  
  public static void createTables() {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      conn = HsqldbConnection.getConnection();
      ps = conn.prepareStatement("CREATE TABLE intraday_stock_summary (   trade_date VARCHAR(8),   trade_time VARCHAR(4),   security_code VARCHAR(21),   board_code VARCHAR(4),   opening_price DECIMAL(14,2),   highest_price DECIMAL(14,2),   lowest_price DECIMAL(14,2),   closing_price DECIMAL(14,2),   previous_price DECIMAL(14,2),   traded_volume BIGINT,   traded_value BIGINT,   traded_frequency INT )");
      ps.executeUpdate();
      ps = conn.prepareStatement("CREATE INDEX IF NOT EXISTS uk_intraday_stock_summary ON intraday_stock_summary   trade_date, trade_time, security_code, board_code )");
      ps.executeUpdate();
      System.out.println("TABLE intraday_stock_summary CREATED...");
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

  /**
   * This function will retrieve today's trade_time that exists on idx_trade but doesn't exist in intraday_stock_summary
   * @return
   */
  public static List<String> getTimeWithoutStockSummary() {
    String now = SDF.format(new Date());
    Connection conn = null;
    PreparedStatement ps = null;
    List<String> listTime = new ArrayList<>();
    try {
      conn = HsqldbConnection.getConnection();
      // Query explanation:
      // 1. SELECT DISTINCT SUBSTRING(trade_time, 1, 4) AS trade_time
      //    FROM idx_trade
      //    WHERE board_code = 'RG' AND trade_date = ?;
      //    This query retrieves unique trade_time values (HHMM format) from the idx_trade table
      //    where board_code is 'RG' and trade_date matches the provided parameter, renaming the result as temp.

      // 2. SELECT DISTINCT trade_time
      //    FROM intraday_stock_summary
      //    WHERE board_code = 'RG' AND trade_date = ?;
      //    This query retrieves unique trade_time values (HHMM format) from the intraday_stock_summary table
      //    where board_code is 'RG' and trade_date matches the provided parameter, renaming the result as ss.

      // 3. This query combines trade_time values from temp (idx_trade) and ss (intraday_stock_summary) using a LEFT JOIN,
      //    selecting trade_time from idx_trade where it does not exist in intraday_stock_summary for the specified trade_date.

      // Conclusion:
      //    In summary, this query identifies trade_time entries from today's idx_trade data for 'RG' board_code
      //    that are not present in the intraday_stock_summary table. It helps in identifying trade times
      //    that require further processing or summarization.
      ps = conn.prepareStatement("SELECT temp.trade_time FROM (SELECT DISTINCT SUBSTRING(trade_time, 1, 4) AS trade_time FROM idx_trade WHERE board_code = 'RG' AND trade_date = ?) AS temp \tLEFT JOIN (SELECT DISTINCT trade_time FROM intraday_stock_summary WHERE board_code = 'RG' AND trade_date = ?) ss ON temp.trade_time = ss.trade_time WHERE ss.trade_time IS NULL");
      ps.setString(1, now);
      ps.setString(2, now);
      ResultSet rs = ps.executeQuery();
      while (rs.next())
        listTime.add(rs.getString("trade_time")); 
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
    return listTime;
  }

  /**
   * Inserts or updates the intraday stock summary data for a specific time into the database.
   * This method aggregates data from IDX_TRADE and inserts it into INTRADAY_STOCK_SUMMARY.
   *
   * @param time The trade time (HHMM format) for which the summary data is processed.
   */
  public static void insertIntradayStockSummary(String time) {
    Connection conn = null;
    PreparedStatement ps = null;
    String now = SDF.format(new Date()); // Current date in YYYYMMDD format

    try {
      conn = HsqldbConnection.getConnection(); // Get database connection
      // Function explanation:
      // This function inserts or updates intraday stock summary data into the INTRADAY_STOCK_SUMMARY table for a specific trade time.
      // It aggregates data from IDX_TRADE based on the provided trade time (HHMM format) and current date (YYYYMMDD).
      // The aggregation includes:
      // 1. Calculating opening_price as the highest price of the first trade for each security_code.
      // 2. Calculating highest_price, lowest_price, and traded_volume for all trades within the trade time.
      // 3. Calculating closing_price as the price of the last trade for each security_code within the trade time.
      // The result is inserted into INTRADAY_STOCK_SUMMARY for further analysis and reporting.
      ps = conn.prepareStatement("INSERT INTO PUBLIC.INTRADAY_STOCK_SUMMARY " +
              "(trade_date, trade_time, security_code, board_code, " +
              "opening_price, highest_price, lowest_price, closing_price, " +
              "traded_volume, traded_value, traded_frequency) " +
              "SELECT " +
              "trade_date, " +
              "trade_time, " +
              "security_code, " +
              "board_code, " +
              "SUM(opening_price) AS opening_price, " +
              "SUM(highest_price) AS highest_price, " +
              "SUM(lowest_price) AS lowest_price, " +
              "SUM(closing_price) AS closing_price, " +
              "SUM(traded_volume) AS traded_volume, " +
              "SUM(traded_value) AS traded_value, " +
              "SUM(traded_frequency) AS traded_frequency " +
              "FROM ( " +
              "    SELECT " +
              "    trade_date, " +
              "    SUBSTRING(trade_time, 1, 4) AS trade_time, " +
              "    security_code, " +
              "    board_code, " +
              "    0 AS opening_price, " +
              "    MAX(price) AS highest_price, " +
              "    MIN(price) AS lowest_price, " +
              "    0 AS closing_price, " +
              "    SUM(volume) AS traded_volume, " +
              "    SUM(volume * price) AS traded_value, " +
              "    COUNT(*) AS traded_frequency " +
              "    FROM IDX_TRADE " +
              "    WHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = ? " +
              "    GROUP BY trade_date, SUBSTRING(trade_time, 1, 4), security_code, BOARD_CODE " +
              "    UNION ALL " +
              "    SELECT " +
              "    trd.trade_date, " +
              "    SUBSTRING(trade_time, 1, 4) AS trade_time, " +
              "    trd.security_code, " +
              "    trd.board_code, " +
              "    trd.price AS opening_price, " +
              "    0 AS highest_price, " +
              "    0 AS lowest_price, " +
              "    0 AS closing_price, " +
              "    0 AS traded_volume, " +
              "    0 AS traded_value, " +
              "    0 AS traded_frequency " +
              "    FROM IDX_TRADE trd, " +
              "    (SELECT MIN(CAST(TRADE_NUMBER AS integer)) AS trade_number " +
              "        FROM IDX_TRADE " +
              "        WHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = ? " +
              "        GROUP BY SUBSTRING(trade_time, 1, 4), security_code) opening_trade " +
              "    WHERE trd.trade_number = opening_trade.trade_number " +
              "    UNION ALL " +
              "    SELECT " +
              "    trd.trade_date, " +
              "    SUBSTRING(trade_time, 1, 4) AS trade_time, " +
              "    trd.security_code, " +
              "    trd.board_code, " +
              "    0 AS opening_price, " +
              "    0 AS highest_price, " +
              "    0 AS lowest_price, " +
              "    trd.price AS closing_price, " +
              "    0 AS traded_volume, " +
              "    0 AS traded_value, " +
              "    0 AS traded_frequency " +
              "    FROM IDX_TRADE trd, " +
              "    (SELECT MAX(CAST(TRADE_NUMBER AS integer)) AS trade_number " +
              "        FROM IDX_TRADE " +
              "        WHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' AND trade_date = ? " +
              "        GROUP BY SUBSTRING(trade_time, 1, 4), security_code) closing_trade " +
              "    WHERE trd.trade_number = closing_trade.trade_number " +
              ") AS temp " +
              "GROUP BY trade_date, trade_time, security_code, board_code ");

      // Set parameters for the prepared statement
      ps.setString(1, time); // Trade time parameter for filtering IDX_TRADE
      ps.setString(2, now); // Current date parameter for filtering IDX_TRADE
      ps.setString(3, time); // Trade time parameter for filtering IDX_TRADE (opening_trade subquery)
      ps.setString(4, now); // Current date parameter for filtering IDX_TRADE (opening_trade subquery)
      ps.setString(5, time); // Trade time parameter for filtering IDX_TRADE (closing_trade subquery)
      ps.setString(6, now); // Current date parameter for filtering IDX_TRADE (closing_trade subquery)

      // Execute the update operation
      ps.executeUpdate();
    } catch (Exception ex) {
      System.out.println("ERROR insertIntradayStockSummary");
      ex.printStackTrace();
    } finally {
      // Close resources in a finally block to ensure they are always closed
      if (ps != null) {
        try {
          ps.close();
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
      // Connection should be closed elsewhere as it may be used by other methods
    }
  }

  
  public static List<IntradayStockSummary> calculateIntradayStockSummary(String time) {
    Connection conn = null;
    PreparedStatement ps = null;
    List<IntradayStockSummary> ssList = new ArrayList<>();
    try {
      conn = HsqldbConnection.getConnection();
      ps = conn.prepareStatement("\tSELECT trade_date, trade_time, security_code, board_code, \t\tSUM(opening_price) AS opening_price, SUM(highest_price) AS highest_price, SUM(lowest_price) AS lowest_price, \t\tSUM(closing_price) AS closing_price, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \t\tSUM(traded_frequency) AS traded_frequency \tFROM ( \t\tSELECT trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, security_code, board_code, \t\t\t0 AS opening_price, MAX(price) AS highest_price, MIN(price) AS lowest_price, 0 AS closing_price, \t\t\tSUM(volume) AS traded_volume, SUM(volume*price) AS traded_value, COUNT(*) AS traded_frequency \t\tFROM IDX_TRADE \t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\tGROUP BY trade_date, SUBSTRING(trade_time, 1, 4), security_code, BOARD_CODE  \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\ttrd.price AS opening_price, 0 AS highest_price, 0 AS lowest_price, 0 AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM IDX_TRADE trd, \t\t\t(SELECT MIN(CAST(TRADE_NUMBER AS integer)) AS trade_number \t\t\tFROM IDX_TRADE \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) opening_trade \t\tWHERE trd.trade_number = opening_trade.trade_number \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\t0 AS opening_price, 0 AS highest_price, 0 AS lowest_price, trd.price AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM IDX_TRADE trd, \t\t\t(SELECT MAX(CAST(TRADE_NUMBER AS integer)) AS trade_number \t\t\tFROM IDX_TRADE \t\t\tWHERE SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) closing_trade \t\tWHERE trd.trade_number = closing_trade.trade_number \t) AS temp \tGROUP BY trade_date, trade_time, security_code, board_code ");
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
  
  public static List<String> getTodayIntradayStockSummary() {
    String now = SDF.format(new Date());
    Connection conn = null;
    PreparedStatement ps = null;
    List<String> stockSummaries = new ArrayList<>();
    try {
      conn = HsqldbConnection.getConnection();
      ps = conn.prepareStatement("SELECT * FROM intraday_stock_summary WHERE TRADE_DATE = ?");
      ps.setString(1, now);
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
  
  public static List<String> getIntradayStockSummary(String time) {
    Connection conn = null;
    PreparedStatement ps = null;
    String now = SDF.format(new Date());
    List<String> stockSummaries = new ArrayList<>();
    try {
      conn = HsqldbConnection.getConnection();
      ps = conn.prepareStatement("SELECT * FROM intraday_stock_summary WHERE trade_time = ? AND trade_date = ? ");
      ps.setString(1, time);
      ps.setString(2, now);
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
  
  public static void checkpointDefrag() {
    Connection conn = null;
    PreparedStatement ps = null;
    try {
      conn = HsqldbConnection.getConnection();
      ps = conn.prepareStatement("CHECKPOINT DEFRAG");
      ps.executeUpdate();
      System.out.println("CHECKPOINT DEFRAG");
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
