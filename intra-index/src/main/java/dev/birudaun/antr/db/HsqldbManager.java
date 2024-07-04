package dev.birudaun.antr.db;

import dev.birudaun.antr.utils.ApplicationProperties;
import java.util.Properties;
import org.hsqldb.Server;
import org.hsqldb.persist.HsqlProperties;

public class HsqldbManager {
  Server hsqlServer;
  
  public void startServer() {
    Properties props = ApplicationProperties.getInstance().getProperties();
    String dbLocation = props.getProperty("hsqldb.dblocation");
    String dbName = props.getProperty("hsqldb.dbname");
    HsqlProperties hsqlProps = new HsqlProperties();
    hsqlProps.setProperty("server.database.0", "file:" + dbLocation);
    hsqlProps.setProperty("server.dbname.0", dbName);
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
