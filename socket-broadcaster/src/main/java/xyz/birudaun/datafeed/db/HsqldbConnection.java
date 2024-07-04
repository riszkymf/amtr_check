package xyz.birudaun.datafeed.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import xyz.birudaun.datafeed.utils.ApplicationProperties;

public class HsqldbConnection {
  private static Connection conn;
  
  static {
    try {
      Class.forName("org.hsqldb.jdbcDriver");
      conn = DriverManager.getConnection(
          System.getenv("HSQLDB_URL"),
          System.getenv("HSQLDB_USERNAME"),
          System.getenv("HSQLDB_PASSWORD"));
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }
  
  public static Connection getConnection() {
    try {
      if (conn == null || conn.isClosed()) {
        conn = DriverManager.getConnection(
            System.getenv("HSQLDB_URL"),
            System.getenv("HSQLDB_USERNAME"),
            System.getenv("HSQLDB_PASSWORD"));
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
    return conn;
  }
}
