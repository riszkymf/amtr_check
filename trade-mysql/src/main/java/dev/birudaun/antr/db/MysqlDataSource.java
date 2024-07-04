package dev.birudaun.antr.db;

import java.util.Properties;

import org.apache.commons.dbcp2.BasicDataSource;

import dev.birudaun.antr.utils.ApplicationProperties;

public class MysqlDataSource {
  private static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";
  
  private static final int CONN_POOL_SIZE = 10;
  
  private BasicDataSource dataSource = new BasicDataSource();
  
  private MysqlDataSource() {
    Properties props = ApplicationProperties.getInstance().getProperties();

    String dbUrl = System.getenv("MYSQL_URL");
    String dbUser = System.getenv("MYSQL_USER");
    String dbPass = System.getenv("MYSQL_PASS");

    this.dataSource.setDriverClassName("com.mysql.jdbc.Driver");
    this.dataSource.setUrl(props.getProperty(dbUrl));
    this.dataSource.setUsername(props.getProperty(dbUser));
    this.dataSource.setPassword(props.getProperty(dbPass));
    this.dataSource.setInitialSize(10);
  }
  
  private static class MysqlDataSourceHolder {
    private static final MysqlDataSource INSTANCE = new MysqlDataSource();
  }
  
  public static MysqlDataSource getInstance() {
    return MysqlDataSourceHolder.INSTANCE;
  }
  
  public BasicDataSource getDataSource() {
    return this.dataSource;
  }
  
  public void setDataSource(BasicDataSource dataSource) {
    this.dataSource = dataSource;
  }
}
