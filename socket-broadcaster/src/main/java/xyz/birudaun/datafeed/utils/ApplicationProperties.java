package xyz.birudaun.datafeed.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class ApplicationProperties {
  private Properties applicationProps = new Properties();
  
  private ApplicationProperties() {
    InputStream defaultStream = null;
    try {
      defaultStream = new FileInputStream("./application.properties");
      this.applicationProps.load(new BufferedReader(new InputStreamReader(defaultStream)));
      defaultStream.close();
    } catch (Exception ex) {
      ex.printStackTrace();
    } 
  }
  
  private static class ApplicationPropertiesHolder {
    public static ApplicationProperties INSTANCE = new ApplicationProperties();
  }
  
  public static ApplicationProperties getInstance() {
    return ApplicationPropertiesHolder.INSTANCE;
  }
  
  public Properties getProperties() {
    return this.applicationProps;
  }
}
