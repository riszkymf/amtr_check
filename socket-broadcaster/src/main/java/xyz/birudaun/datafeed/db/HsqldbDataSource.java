package xyz.birudaun.datafeed.db;

import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;
import xyz.birudaun.datafeed.utils.ApplicationProperties;

public class HsqldbDataSource {
  private static final String DRIVER_CLASS_NAME = "org.hsqldb.jdbcDriver";
  
  private static final int CONN_POOL_SIZE = 10;
  
  private BasicDataSource dataSource = new BasicDataSource();
  
  private HsqldbDataSource() {
    Properties props = ApplicationProperties.getInstance().getProperties();
    this.dataSource.setDriverClassName("org.hsqldb.jdbcDriver");
    this.dataSource.setUrl(System.getenv("HSQLDB_URL"));
    this.dataSource.setUsername(System.getenv("HSQLDB_USERNAME"));
    this.dataSource.setPassword(System.getenv("HSQLDB_PASSWORD"));
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
