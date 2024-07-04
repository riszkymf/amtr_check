package xyz.birudaun.datafeed.db;

import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import xyz.birudaun.datafeed.utils.CommonUtils;

public class IdxdailyDataOperations {
  private static final Logger _log = Logger.getLogger(DbOperations.class);
  
  private static final String SQL_truncateNbsa = "TRUNCATE TABLE idxdaily.eod_nbsa";
  
  private static final String SQL_insertNbsaDataByDate = "INSERT INTO idxdaily.eod_nbsa (datafeed_date, security_code, net_buysell) \tSELECT datafeed_date, security_code, (SUM(buy_value) - SUM(sell_value)) AS net_buysell \tFROM ( \t\tSELECT datafeed_date, security_code, SUM(price * volume) AS buy_value, 0 AS sell_value \t\tFROM idxdaily.idx_trade  \t\tWHERE board_code = 'RG' AND buyer_type = 'F' \t\tGROUP BY datafeed_date, security_code \t\tUNION ALL \t\tSELECT datafeed_date, security_code, 0 AS buy_value, SUM(price * volume) AS sell_value \t\tFROM idxdaily.idx_trade  \t\tWHERE board_code = 'RG' AND seller_type = 'F' \t\tGROUP BY datafeed_date, security_code \t) AS temp \tGROUP BY datafeed_date, security_code ";
  
  private static final String SQL_truncateBidOff = "TRUNCATE TABLE idxdaily.eod_bid_off";
  
  private static final String SQL_insertBidOffDataByDate = "INSERT INTO idxdaily.eod_bid_off (datafeed_date, security_code, bid_off) \tSELECT datafeed_date, security_code, SUM(buy_value - sell_value) AS bid_off \tFROM ( \t\tSELECT datafeed_date, security_code, buyer_code AS broker_code, \t\t\tSUM(price * volume) AS buy_value, 0 AS sell_value \t\tFROM idxdaily.idx_trade \t\tWHERE transaction_type = 'BUY' AND board_code = 'RG' \t\tGROUP BY datafeed_date, security_code, buyer_code \t\tUNION ALL \t\tSELECT datafeed_date, security_code, seller_code AS broker_code, \t\t\t0 AS buy_value, SUM(price * volume) AS sell_value \t\tFROM idxdaily.idx_trade \t\tWHERE transaction_type = 'SELL' AND board_code = 'RG' \t\tGROUP BY datafeed_date, security_code, seller_code \t) AS temp \tGROUP BY datafeed_date, security_code";
  
  private static final String SQL_truncateTopBrokerValue = "TRUNCATE TABLE idxdaily.eod_top_broker_value";
  
  private static final String SQL_insertTopBrokerDataByDate = "INSERT INTO idxdaily.eod_top_broker_value (datafeed_date, security_code, broker_code, buy_value, sell_value) \tSELECT datafeed_date, security_code, broker_code, SUM(buy_value) AS buy_value, SUM(sell_value) AS sell_value \tFROM ( \t\tSELECT datafeed_date, security_code, buyer_code AS broker_code,  \t\t\tSUM(volume * price) AS buy_value, 0 AS sell_value \t\tFROM idxdaily.idx_trade  \t\tWHERE board_code = 'RG' \t\tGROUP BY datafeed_date, security_code, buyer_code \t\tUNION ALL \t\tSELECT datafeed_date, security_code, seller_code AS broker_code,  \t\t\t0 AS buy_value, SUM(volume * price) AS sell_value \t\tFROM idxdaily.idx_trade  \t\tWHERE board_code = 'RG' \t\tGROUP BY datafeed_date, security_code, seller_code \t) AS temp \tGROUP BY datafeed_date, security_code, broker_code";
  
  private static final String SQL_truncateTop8BrokerValue = "TRUNCATE TABLE idxdaily.eod_top8_broker_value";
  
  private static final String SQL_insertTop8BrokerDataByDate = "INSERT INTO idxdaily.eod_top8_broker_value (datafeed_date, security_code, net_value) \tSELECT datafeed_date, security_code, SUM(buy_value) - SUM(sell_value) AS net_value \tFROM ( \t\tSELECT tbv.datafeed_date, tbv.security_code, SUM(buy_value) AS buy_value, 0 AS sell_value \t\tFROM \t\t\tidxdaily.eod_top_broker_value tbv \t\t\tINNER JOIN ( \t\t\t\tSELECT   datafeed_date, security_code, GROUP_CONCAT(broker_code ORDER BY buy_value DESC) grouped_broker \t\t\t\tFROM     idxdaily.eod_top_broker_value \t\t\t\tGROUP BY datafeed_date, security_code \t\t\t) group_max  \t\t\t\tON (tbv.datafeed_date = group_max.datafeed_date AND tbv.security_code = group_max.security_code) \t\t\tAND FIND_IN_SET(broker_code, grouped_broker) BETWEEN 1 AND 8 \t\tGROUP BY tbv.datafeed_date, tbv.security_code \t\tUNION ALL \t\tSELECT tbv.datafeed_date, tbv.security_code, 0 AS buy_value, SUM(sell_value) \t\tFROM \t\t\tidxdaily.eod_top_broker_value tbv \t\t\tINNER JOIN ( \t\t\t\tSELECT   datafeed_date, security_code, GROUP_CONCAT(broker_code ORDER BY sell_value DESC) grouped_broker \t\t\t\tFROM     idxdaily.eod_top_broker_value \t\t\t\tGROUP BY datafeed_date, security_code \t\t\t) group_max  \t\t\t\tON (tbv.datafeed_date = group_max.datafeed_date AND tbv.security_code = group_max.security_code) \t\t\tAND FIND_IN_SET(broker_code, grouped_broker) BETWEEN 1 AND 8 \t\tGROUP BY tbv.datafeed_date, tbv.security_code \t) AS temp \tGROUP BY datafeed_date, security_code \tORDER BY datafeed_date, security_code";
  
  private static final String SQL_insertIntradayStockSummary = "INSERT INTO idx_data.intraday_stock_summary (trade_date, trade_time, security_code, board_code, opening_price, highest_price, \tlowest_price, closing_price, traded_volume, traded_value, traded_frequency) \tSELECT trade_date, trade_time, security_code, board_code, \t\tSUM(opening_price) AS opening_price, SUM(highest_price) AS highest_price, SUM(lowest_price) AS lowest_price, \t\tSUM(closing_price) AS closing_price, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \t\tSUM(traded_frequency) AS traded_frequency \tFROM ( \t\tSELECT trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, security_code, board_code, \t\t\t0 AS opening_price, MAX(price) AS highest_price, MIN(price) AS lowest_price, 0 AS closing_price, \t\t\tSUM(volume) AS traded_volume, SUM(volume*price) AS traded_value, COUNT(*) AS traded_frequency \t\tFROM idx_data.idx_trade \t\tWHERE trade_date = CURRENT_DATE AND SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\tGROUP BY trade_date, SUBSTRING(trade_time, 1, 4), security_code \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\ttrd.price AS opening_price, 0 AS highest_price, 0 AS lowest_price, 0 AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM idx_data.idx_trade trd, \t\t\t(SELECT MIN(trade_number) AS trade_number \t\t\tFROM idx_data.idx_trade \t\t\tWHERE trade_date = CURRENT_DATE AND SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) opening_trade \t\tWHERE trd.trade_number = opening_trade.trade_number \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\t0 AS opening_price, 0 AS highest_price, 0 AS lowest_price, trd.price AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM idx_data.idx_trade trd, \t\t\t(SELECT MAX(trade_number) AS trade_number \t\t\tFROM idx_data.idx_trade \t\t\tWHERE trade_date = CURRENT_DATE AND SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) closing_trade \t\tWHERE trd.trade_number = closing_trade.trade_number \t) AS temp \tGROUP BY trade_time, security_code ";
  
  private static final String SQL_getIntradayStockSummary = "SELECT * FROM intraday_stock_summary WHERE trade_date = CURRENT_DATE AND trade_time = ?";
  
  private static final String SQL_getTimeWithoutStockSummary = "SELECT temp.trade_time FROM (SELECT DISTINCT SUBSTRING(trade_time, 1, 4) AS trade_time FROM idx_trade WHERE board_code = 'RG') AS temp \tLEFT JOIN (SELECT DISTINCT trade_time FROM intraday_stock_summary WHERE board_code = 'RG') ss ON temp.trade_time = ss.trade_time WHERE ss.trade_time IS NULL";
  
  public static void insertNbsaDataByDate() {
    PreparedStatement ps = null;
    try {
      Connection conn = DbConnection.getConnection();
      ps = conn.prepareStatement("TRUNCATE TABLE idxdaily.eod_nbsa");
      ps.executeUpdate();
      ps = conn.prepareStatement("INSERT INTO idxdaily.eod_nbsa (datafeed_date, security_code, net_buysell) \tSELECT datafeed_date, security_code, (SUM(buy_value) - SUM(sell_value)) AS net_buysell \tFROM ( \t\tSELECT datafeed_date, security_code, SUM(price * volume) AS buy_value, 0 AS sell_value \t\tFROM idxdaily.idx_trade  \t\tWHERE board_code = 'RG' AND buyer_type = 'F' \t\tGROUP BY datafeed_date, security_code \t\tUNION ALL \t\tSELECT datafeed_date, security_code, 0 AS buy_value, SUM(price * volume) AS sell_value \t\tFROM idxdaily.idx_trade  \t\tWHERE board_code = 'RG' AND seller_type = 'F' \t\tGROUP BY datafeed_date, security_code \t) AS temp \tGROUP BY datafeed_date, security_code ");
      ps.executeUpdate();
    } catch (MySQLIntegrityConstraintViolationException mySQLIntegrityConstraintViolationException) {
    
    } catch (Exception ex) {
      System.out.println("ERROR insertNbsaDataByDate");
      ex.printStackTrace();
      _log.error("ERROR insertNbsaDataByDate", ex);
    } finally {
      if (ps != null)
        try {
          ps.close();
        } catch (Exception ex) {
          ex.printStackTrace();
        }  
    } 
  }
  
  public static void insertBidOffDataByDate() {
    PreparedStatement ps = null;
    try {
      Connection conn = DbConnection.getConnection();
      ps = conn.prepareStatement("TRUNCATE TABLE idxdaily.eod_bid_off");
      ps.executeUpdate();
      ps = conn.prepareStatement("INSERT INTO idxdaily.eod_bid_off (datafeed_date, security_code, bid_off) \tSELECT datafeed_date, security_code, SUM(buy_value - sell_value) AS bid_off \tFROM ( \t\tSELECT datafeed_date, security_code, buyer_code AS broker_code, \t\t\tSUM(price * volume) AS buy_value, 0 AS sell_value \t\tFROM idxdaily.idx_trade \t\tWHERE transaction_type = 'BUY' AND board_code = 'RG' \t\tGROUP BY datafeed_date, security_code, buyer_code \t\tUNION ALL \t\tSELECT datafeed_date, security_code, seller_code AS broker_code, \t\t\t0 AS buy_value, SUM(price * volume) AS sell_value \t\tFROM idxdaily.idx_trade \t\tWHERE transaction_type = 'SELL' AND board_code = 'RG' \t\tGROUP BY datafeed_date, security_code, seller_code \t) AS temp \tGROUP BY datafeed_date, security_code");
      ps.executeUpdate();
    } catch (MySQLIntegrityConstraintViolationException mySQLIntegrityConstraintViolationException) {
    
    } catch (Exception ex) {
      System.out.println("ERROR insertBidOffDataByDate");
      ex.printStackTrace();
      _log.error("ERROR insertBidOffDataByDate", ex);
    } finally {
      if (ps != null)
        try {
          ps.close();
        } catch (Exception ex) {
          ex.printStackTrace();
        }  
    } 
  }
  
  public static void insertTopBrokerDataByDate() {
    PreparedStatement ps = null;
    try {
      Connection conn = DbConnection.getConnection();
      ps = conn.prepareStatement("TRUNCATE TABLE idxdaily.eod_top_broker_value");
      ps.executeUpdate();
      ps = conn.prepareStatement("INSERT INTO idxdaily.eod_top_broker_value (datafeed_date, security_code, broker_code, buy_value, sell_value) \tSELECT datafeed_date, security_code, broker_code, SUM(buy_value) AS buy_value, SUM(sell_value) AS sell_value \tFROM ( \t\tSELECT datafeed_date, security_code, buyer_code AS broker_code,  \t\t\tSUM(volume * price) AS buy_value, 0 AS sell_value \t\tFROM idxdaily.idx_trade  \t\tWHERE board_code = 'RG' \t\tGROUP BY datafeed_date, security_code, buyer_code \t\tUNION ALL \t\tSELECT datafeed_date, security_code, seller_code AS broker_code,  \t\t\t0 AS buy_value, SUM(volume * price) AS sell_value \t\tFROM idxdaily.idx_trade  \t\tWHERE board_code = 'RG' \t\tGROUP BY datafeed_date, security_code, seller_code \t) AS temp \tGROUP BY datafeed_date, security_code, broker_code");
      ps.executeUpdate();
    } catch (MySQLIntegrityConstraintViolationException mySQLIntegrityConstraintViolationException) {
    
    } catch (Exception ex) {
      System.out.println("ERROR insertTopBrokerDataByDate");
      ex.printStackTrace();
      _log.error("ERROR insertTopBrokerDataByDate", ex);
    } finally {
      if (ps != null)
        try {
          ps.close();
        } catch (Exception ex) {
          ex.printStackTrace();
        }  
    } 
  }
  
  public static void insertTop8BrokerDataByDate() {
    PreparedStatement ps = null;
    try {
      Connection conn = DbConnection.getConnection();
      ps = conn.prepareStatement("TRUNCATE TABLE idxdaily.eod_top8_broker_value");
      ps.executeUpdate();
      ps = conn.prepareStatement("INSERT INTO idxdaily.eod_top8_broker_value (datafeed_date, security_code, net_value) \tSELECT datafeed_date, security_code, SUM(buy_value) - SUM(sell_value) AS net_value \tFROM ( \t\tSELECT tbv.datafeed_date, tbv.security_code, SUM(buy_value) AS buy_value, 0 AS sell_value \t\tFROM \t\t\tidxdaily.eod_top_broker_value tbv \t\t\tINNER JOIN ( \t\t\t\tSELECT   datafeed_date, security_code, GROUP_CONCAT(broker_code ORDER BY buy_value DESC) grouped_broker \t\t\t\tFROM     idxdaily.eod_top_broker_value \t\t\t\tGROUP BY datafeed_date, security_code \t\t\t) group_max  \t\t\t\tON (tbv.datafeed_date = group_max.datafeed_date AND tbv.security_code = group_max.security_code) \t\t\tAND FIND_IN_SET(broker_code, grouped_broker) BETWEEN 1 AND 8 \t\tGROUP BY tbv.datafeed_date, tbv.security_code \t\tUNION ALL \t\tSELECT tbv.datafeed_date, tbv.security_code, 0 AS buy_value, SUM(sell_value) \t\tFROM \t\t\tidxdaily.eod_top_broker_value tbv \t\t\tINNER JOIN ( \t\t\t\tSELECT   datafeed_date, security_code, GROUP_CONCAT(broker_code ORDER BY sell_value DESC) grouped_broker \t\t\t\tFROM     idxdaily.eod_top_broker_value \t\t\t\tGROUP BY datafeed_date, security_code \t\t\t) group_max  \t\t\t\tON (tbv.datafeed_date = group_max.datafeed_date AND tbv.security_code = group_max.security_code) \t\t\tAND FIND_IN_SET(broker_code, grouped_broker) BETWEEN 1 AND 8 \t\tGROUP BY tbv.datafeed_date, tbv.security_code \t) AS temp \tGROUP BY datafeed_date, security_code \tORDER BY datafeed_date, security_code");
      ps.executeUpdate();
    } catch (MySQLIntegrityConstraintViolationException mySQLIntegrityConstraintViolationException) {
    
    } catch (Exception ex) {
      System.out.println("ERROR insertTop8BrokerDataByDate");
      ex.printStackTrace();
      _log.error("ERROR insertTop8BrokerDataByDate", ex);
    } finally {
      if (ps != null)
        try {
          ps.close();
        } catch (Exception ex) {
          ex.printStackTrace();
        }  
    } 
  }
  
  public static void insertIntradayStockSummary(String time) {
    PreparedStatement ps = null;
    try {
      Connection conn = DbConnection.getConnection();
      ps = conn.prepareStatement("INSERT INTO idx_data.intraday_stock_summary (trade_date, trade_time, security_code, board_code, opening_price, highest_price, \tlowest_price, closing_price, traded_volume, traded_value, traded_frequency) \tSELECT trade_date, trade_time, security_code, board_code, \t\tSUM(opening_price) AS opening_price, SUM(highest_price) AS highest_price, SUM(lowest_price) AS lowest_price, \t\tSUM(closing_price) AS closing_price, SUM(traded_volume) AS traded_volume, SUM(traded_value) AS traded_value, \t\tSUM(traded_frequency) AS traded_frequency \tFROM ( \t\tSELECT trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, security_code, board_code, \t\t\t0 AS opening_price, MAX(price) AS highest_price, MIN(price) AS lowest_price, 0 AS closing_price, \t\t\tSUM(volume) AS traded_volume, SUM(volume*price) AS traded_value, COUNT(*) AS traded_frequency \t\tFROM idx_data.idx_trade \t\tWHERE trade_date = CURRENT_DATE AND SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\tGROUP BY trade_date, SUBSTRING(trade_time, 1, 4), security_code \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\ttrd.price AS opening_price, 0 AS highest_price, 0 AS lowest_price, 0 AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM idx_data.idx_trade trd, \t\t\t(SELECT MIN(trade_number) AS trade_number \t\t\tFROM idx_data.idx_trade \t\t\tWHERE trade_date = CURRENT_DATE AND SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) opening_trade \t\tWHERE trd.trade_number = opening_trade.trade_number \t\tUNION ALL \t\tSELECT trd.trade_date, SUBSTRING(trade_time, 1, 4) AS trade_time, trd.security_code, trd.board_code, \t\t\t0 AS opening_price, 0 AS highest_price, 0 AS lowest_price, trd.price AS closing_price, \t\t\t0 AS traded_volume, 0 AS traded_value, 0 AS traded_frequency \t\tFROM idx_data.idx_trade trd, \t\t\t(SELECT MAX(trade_number) AS trade_number \t\t\tFROM idx_data.idx_trade \t\t\tWHERE trade_date = CURRENT_DATE AND SUBSTRING(trade_time, 1, 4) = ? AND board_code = 'RG' \t\t\tGROUP BY SUBSTRING(trade_time, 1, 4), security_code) closing_trade \t\tWHERE trd.trade_number = closing_trade.trade_number \t) AS temp \tGROUP BY trade_time, security_code ");
      ps.setString(1, time);
      ps.setString(2, time);
      ps.setString(3, time);
      ps.executeUpdate();
    } catch (Exception ex) {
      System.out.println("ERROR insertTop8BrokerDataByDate");
      ex.printStackTrace();
      _log.error("ERROR insertTop8BrokerDataByDate", ex);
    } finally {
      if (ps != null)
        try {
          ps.close();
        } catch (Exception ex) {
          ex.printStackTrace();
        }  
    } 
  }
  
  public static List<String> getIntradayStockSummary(String time) {
    List<String> stockSummaries = new ArrayList<>();
    try {
      Connection conn = DbConnection.getConnection();
      PreparedStatement ps = conn.prepareStatement("SELECT * FROM intraday_stock_summary WHERE trade_date = CURRENT_DATE AND trade_time = ?");
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
    } 
    return stockSummaries;
  }
  
  public static List<String> getTimeWithoutStockSummary() {
    List<String> listTime = new ArrayList<>();
    try {
      Connection conn = DbConnection.getConnection();
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT temp.trade_time FROM (SELECT DISTINCT SUBSTRING(trade_time, 1, 4) AS trade_time FROM idx_trade WHERE board_code = 'RG') AS temp \tLEFT JOIN (SELECT DISTINCT trade_time FROM intraday_stock_summary WHERE board_code = 'RG') ss ON temp.trade_time = ss.trade_time WHERE ss.trade_time IS NULL");
      while (rs.next())
        listTime.add(rs.getString("trade_time")); 
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
    return listTime;
  }
}
