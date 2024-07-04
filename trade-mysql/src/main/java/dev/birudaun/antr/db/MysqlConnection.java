package dev.birudaun.antr.db;

import dev.birudaun.antr.utils.ApplicationProperties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class MysqlConnection {
  private static Connection conn;
  
  static {
    try {
      Properties props = ApplicationProperties.getInstance().getProperties();
      Class.forName("com.mysql.jdbc.Driver");
      conn = DriverManager.getConnection(
          props.getProperty("mysql.url"), 
          props.getProperty("mysql.username"), 
          props.getProperty("mysql.password"));
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }
  
  public static Connection getConnection() {
    try {
      if (conn == null || conn.isClosed()) {
        Properties props = ApplicationProperties.getInstance().getProperties();
        conn = DriverManager.getConnection(
            props.getProperty("mysql.url"), 
            props.getProperty("mysql.username"), 
            props.getProperty("mysql.password"));
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
    return conn;
  }
}
