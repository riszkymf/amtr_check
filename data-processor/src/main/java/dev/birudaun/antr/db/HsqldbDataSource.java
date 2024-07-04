package dev.birudaun.antr.db;

import dev.birudaun.antr.utils.ApplicationProperties;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;

public class HsqldbDataSource {
  private static final String DRIVER_CLASS_NAME = "org.hsqldb.jdbcDriver";
  
  private static final int CONN_POOL_SIZE = 10;
  
  private BasicDataSource dataSource = new BasicDataSource();
  
  private HsqldbDataSource() {
    Properties props = ApplicationProperties.getInstance().getProperties();
    this.dataSource.setDriverClassName("org.hsqldb.jdbcDriver");
    this.dataSource.setUrl(props.getProperty("hsqldb.url"));
    this.dataSource.setUsername(props.getProperty("hsqldb.username"));
    this.dataSource.setPassword(props.getProperty("hsqldb.password"));
    this.dataSource.setInitialSize(10);
  }
  
  private static class HsqlDataSourceHolder {
    private static final HsqldbDataSource INSTANCE = new HsqldbDataSource();
  }
  
  public static HsqldbDataSource getInstance() {
    return HsqlDataSourceHolder.INSTANCE;
  }
  
  public BasicDataSource getDataSource() {
    return this.dataSource;
  }
  
  public void setDataSource(BasicDataSource dataSource) {
    this.dataSource = dataSource;
  }
}
