package xyz.birudaun.datafeed.db;

import java.util.Properties;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;
import xyz.birudaun.datafeed.utils.ApplicationProperties;

public class HsqldbManager {
  Server hsqlServer;
  
  public void startServer() {
    Properties props = ApplicationProperties.getInstance().getProperties();
//    String dbLocation = props.getProperty("hsqldb.dblocation");
//    String dbName = props.getProperty("hsqldb.dbname");
    String dbLocation = System.getenv("HSQLDB_LOCATION");
    String dbName = System.getenv("HSQLDB_DBNAME");
    System.out.println(dbName+" "+dbLocation);
    HsqlProperties hsqlProps = new HsqlProperties();
    hsqlProps.setProperty("server.database.0", "file:" + dbLocation);
    hsqlProps.setProperty("server.dbname.0", dbName);
    System.out.println(hsqlProps.getProperties());
    System.out.println(hsqlProps.getProperty("server.database.0"));
    System.out.println(hsqlProps.getProperty("server.dbname.0"));
    this.hsqlServer = (Server)new Server();
    try {
      this.hsqlServer.setProperties(hsqlProps);
    } catch (Exception e) {
      return;
    } 
    this.hsqlServer.start();
  }
  
  public void stopServer() {
    this.hsqlServer.shutdown();
  }
}
