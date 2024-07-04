package xyz.birudaun.datafeed.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;
import xyz.birudaun.datafeed.utils.CommonUtils;

public class DbOperations {
  private static final Logger _log = Logger.getLogger(DbOperations.class);
  
  private static final String SQL_getIdxStockSummary = "SELECT security_code, datafeed_date, TIME, opening_price, highest_price, lowest_price, closing_price, traded_volume, \t(nbsa/1000) AS nbsa, (bid_off/1000) AS bid_off, (top8/1000) AS top8 FROM portaldata.v_eod_stock_summary";
  
  private static final String SQL_getIdxStockSummaryLast3Days = "select ss.security_code, ss.datafeed_date, ss.time, ss.opening_price, ss.highest_price, ss.lowest_price, \tss.closing_price, ss.traded_volume, \t(ss.nbsa/1000) AS nbsa, (ss.bid_off/1000) AS bid_off, (ss.top8/1000) AS top8 FROM idxtemp.v_eod_stock_summary ss \tINNER JOIN ( \t\tSELECT DISTINCT datafeed_date  \t\tFROM idxtemp.idx_stock_summary  \t\tORDER BY datafeed_date DESC LIMIT 3 \t) AS ss2 ON ss.datafeed_date = ss2.datafeed_date ORDER BY ss.datafeed_date, ss.security_code";
  
  private static final String SQL_getIdxStockSummaryBackfill = "SELECT security_code, datafeed_date, time, opening_price, highest_price, lowest_price, closing_price, traded_volume, \t(nbsa/1000) as nbsa, (bid_off/1000) as bid_off, (top8/1000) as top8 from idxtemp.v_eod_stock_summary";
  
  private static final String SQL_getIdxStockIndustryClassification = "select * from idxdaily.v_stock_sector";
  
  public static List<String> getIdxStockSummary() {
    List<String> stockSummaries = new ArrayList<>();
    try {
      Connection conn = DbConnection.getConnection();
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT security_code, datafeed_date, TIME, opening_price, highest_price, lowest_price, closing_price, traded_volume, \t(nbsa/1000) AS nbsa, (bid_off/1000) AS bid_off, (top8/1000) AS top8 FROM portaldata.v_eod_stock_summary");
      String DATE = (new SimpleDateFormat("yyyy/MM/dd")).format(new Date());
      while (rs.next()) {
        String stockSummary = 
          String.valueOf(rs.getString("security_code")) + 
          "," + DATE + 
          "," + (new SimpleDateFormat("HH:mm")).format(new Date()) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("opening_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("highest_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("lowest_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("closing_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("traded_volume")) + 
          "," + (new DecimalFormat("###0.00")).format(rs.getDouble("nbsa")) + 
          "," + (new DecimalFormat("###0.00")).format(rs.getDouble("bid_off")) + 
          "," + (new DecimalFormat("###0.00")).format(rs.getDouble("top8"));
        stockSummaries.add(stockSummary);
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
    return stockSummaries;
  }
  
  public static List<String> getIdxIntradayStockSummaryLast3Days() {
    List<String> stockSummaries = new ArrayList<>();
    try {
      Connection conn = DbConnection.getConnection();
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("select ss.security_code, ss.datafeed_date, ss.time, ss.opening_price, ss.highest_price, ss.lowest_price, \tss.closing_price, ss.traded_volume, \t(ss.nbsa/1000) AS nbsa, (ss.bid_off/1000) AS bid_off, (ss.top8/1000) AS top8 FROM idxtemp.v_eod_stock_summary ss \tINNER JOIN ( \t\tSELECT DISTINCT datafeed_date  \t\tFROM idxtemp.idx_stock_summary  \t\tORDER BY datafeed_date DESC LIMIT 3 \t) AS ss2 ON ss.datafeed_date = ss2.datafeed_date ORDER BY ss.datafeed_date, ss.security_code");
      while (rs.next()) {
        String stockSummary = 
          String.valueOf(rs.getString("security_code")) + 
          "," + CommonUtils.getDateWithSeparator(rs.getString("datafeed_date"), "/") + 
          ",00:00" + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("opening_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("highest_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("lowest_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("closing_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("traded_volume")) + 
          "," + (new DecimalFormat("###0.00")).format(rs.getDouble("nbsa")) + 
          "," + (new DecimalFormat("###0.00")).format(rs.getDouble("bid_off")) + 
          "," + (new DecimalFormat("###0.00")).format(rs.getDouble("top8"));
        stockSummaries.add(stockSummary);
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
    return stockSummaries;
  }
  
  public static List<String> getIdxIntradayStockSummaryBackfill() {
    List<String> stockSummaries = new ArrayList<>();
    try {
      Connection conn = DbConnection.getConnection();
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT security_code, datafeed_date, time, opening_price, highest_price, lowest_price, closing_price, traded_volume, \t(nbsa/1000) as nbsa, (bid_off/1000) as bid_off, (top8/1000) as top8 from idxtemp.v_eod_stock_summary");
      while (rs.next()) {
        String stockSummary = 
          String.valueOf(rs.getString("security_code")) + 
          "," + CommonUtils.getDateWithSeparator(rs.getString("datafeed_date"), "/") + 
          ",00:00" + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("opening_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("highest_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("lowest_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("closing_price")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("traded_volume")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("nbsa")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("bid_off")) + 
          "," + (new DecimalFormat("###0")).format(rs.getDouble("top8"));
        stockSummaries.add(stockSummary);
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
    return stockSummaries;
  }
  
  public static List<String> getIdxStockIndustryClassification() {
    List<String> stockIndustryClassificationList = new ArrayList<>();
    try {
      Connection conn = DbConnection.getConnection();
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("select * from idxdaily.v_stock_sector");
      while (rs.next()) {
        String stockSummary = 
          "00|" + 
          rs.getString("security_code") + 
          "|" + rs.getString("security_name") + 
          "|IDX" + 
          "|-" + 
          "|" + rs.getString("sector") + 
          "|" + rs.getString("industry");
        stockIndustryClassificationList.add(stockSummary);
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
    return stockIndustryClassificationList;
  }
}
