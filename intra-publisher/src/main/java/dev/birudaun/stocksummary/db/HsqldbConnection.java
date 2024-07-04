package dev.birudaun.stocksummary.db;

import dev.birudaun.utils.ApplicationProperties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class HsqldbConnection {
  private static Connection conn;
  
  static {
    try {
      Properties props = ApplicationProperties.getInstance().getProperties();
      Class.forName("org.hsqldb.jdbcDriver");
      conn = DriverManager.getConnection(
          props.getProperty("hsqldb.url"), 
          props.getProperty("hsqldb.username"), 
          props.getProperty("hsqldb.password"));
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }
  
  public static Connection getConnection() {
    try {
      if (conn == null || conn.isClosed()) {
        Properties props = ApplicationProperties.getInstance().getProperties();
        conn = DriverManager.getConnection(
            props.getProperty("hsqldb.url"), 
            props.getProperty("hsqldb.username"), 
            props.getProperty("hsqldb.password"));
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
    return conn;
  }
}
