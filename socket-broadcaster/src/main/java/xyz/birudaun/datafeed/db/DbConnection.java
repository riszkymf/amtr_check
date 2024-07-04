package xyz.birudaun.datafeed.db;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import org.apache.log4j.Logger;

public class DbConnection {
  private static Connection conn;
  
  private static Properties props;
  
  private static final Logger _log = Logger.getLogger(DbConnection.class);
  
  static {
    try {
      InputStream input = new FileInputStream("application.properties");
      Class.forName("com.mysql.jdbc.Driver");
//      conn = DriverManager.getConnection(
//          props.getProperty("mysql.url"),
//          props.getProperty("mysql.username"),
//          props.getProperty("mysql.password"));
      conn = DriverManager.getConnection(
              System.getenv("MYSQL_URL"),
              System.getenv("MYSQL_USERNAME"),
              System.getenv("MYSQL_PASSWORD"));
    } catch (Exception e) {
      e.printStackTrace();
      _log.error("ERROR create mysql connection", e);
    } 
  }
  
  public static Connection getConnection() {
    if (conn == null)
      try {
        conn = DriverManager.getConnection(
                System.getenv("MYSQL_URL"),
                System.getenv("MYSQL_USERNAME"),
                System.getenv("MYSQL_PASSWORD"));
      } catch (Exception e) {
        e.printStackTrace();
        _log.error("ERROR create mysql connection", e);
      }  
    return conn;
  }
}
