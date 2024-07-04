package dev.birudaun.antr.db;

import dev.birudaun.antr.utils.ApplicationProperties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

public class PostgresConnection {
  private static Connection conn;
  
  static {
    try {
      Properties props = ApplicationProperties.getInstance().getProperties();
      Class.forName("org.postgresql.Driver");
      conn = DriverManager.getConnection(
          props.getProperty("postgres.url"), 
          props.getProperty("postgres.username"), 
          props.getProperty("postgres.password"));
    } catch (Exception e) {
      e.printStackTrace();
    } 
  }
  
  public static Connection getConnection() {
    try {
      if (conn == null || conn.isClosed()) {
        Properties props = ApplicationProperties.getInstance().getProperties();
        conn = DriverManager.getConnection(
            props.getProperty("postgres.url"), 
            props.getProperty("postgres.username"), 
            props.getProperty("postgres.password"));
      } 
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
    return conn;
  }
}
