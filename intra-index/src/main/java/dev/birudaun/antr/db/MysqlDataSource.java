package dev.birudaun.antr.db;

import dev.birudaun.antr.utils.ApplicationProperties;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class MysqlDataSource {
  private static final String DRIVER_CLASS_NAME = "com.mysql.jdbc.Driver";
  
  private static final int CONN_POOL_SIZE = 10;
  
  private BasicDataSource dataSource = new BasicDataSource();
  
  private MysqlDataSource() {
    Properties props = ApplicationProperties.getInstance().getProperties();
    this.dataSource.setDriverClassName("com.mysql.jdbc.Driver");
    this.dataSource.setUrl(props.getProperty("mysql.url"));
    this.dataSource.setUsername(props.getProperty("mysql.username"));
    this.dataSource.setPassword(props.getProperty("mysql.password"));
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
