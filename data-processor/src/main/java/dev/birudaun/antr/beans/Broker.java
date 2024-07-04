package dev.birudaun.antr.beans;

import org.apache.commons.lang3.StringUtils;

public class Broker {
  private String brokerCode;
  
  private String brokerName;
  
  private String brokerStatus;
  
  public String getBrokerCode() {
    return this.brokerCode;
  }
  
  public void setBrokerCode(String brokerCode) {
    this.brokerCode = StringUtils.trim(brokerCode);
  }
  
  public String getBrokerName() {
    return this.brokerName;
  }
  
  public void setBrokerName(String brokerName) {
    this.brokerName = StringUtils.trim(brokerName);
  }
  
  public String getBrokerStatus() {
    return this.brokerStatus;
  }
  
  public void setBrokerStatus(String brokerStatus) {
    this.brokerStatus = brokerStatus;
  }
  
  public String convertDataStream() {
    return "ANTR|B|" + this.brokerCode + "|" + this.brokerName + "|" + this.brokerStatus;
  }
  
  public String toString() {
    return "Broker [brokerCode=" + this.brokerCode + ", brokerName=" + this.brokerName + ", brokerStatus=" + this.brokerStatus + 
      "]";
  }
}
