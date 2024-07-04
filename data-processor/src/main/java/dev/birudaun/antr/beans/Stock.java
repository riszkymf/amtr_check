package dev.birudaun.antr.beans;

import org.apache.commons.lang3.StringUtils;

public class Stock {
  private String securityCode;
  
  private String securityName;
  
  private String securityStatus;
  
  private String securityType;
  
  private String subSector;
  
  private double ipoPrice;
  
  private double basePrice;
  
  private long listedShares;
  
  private long tradableListedShares;
  
  private int sharesPerLot;
  
  private String remarks;
  
  private String securityRemarks2;
  
  private double weight;
  
  public String getSecurityCode() {
    return this.securityCode;
  }
  
  public void setSecurityCode(String securityCode) {
    this.securityCode = StringUtils.trim(securityCode);
  }
  
  public String getSecurityName() {
    return this.securityName;
  }
  
  public void setSecurityName(String securityName) {
    this.securityName = StringUtils.trim(securityName);
  }
  
  public String getSecurityStatus() {
    return this.securityStatus;
  }
  
  public void setSecurityStatus(String securityStatus) {
    this.securityStatus = securityStatus;
  }
  
  public String getSecurityType() {
    return this.securityType;
  }
  
  public void setSecurityType(String securityType) {
    this.securityType = StringUtils.trim(securityType);
  }
  
  public String getSubSector() {
    return this.subSector;
  }
  
  public void setSubSector(String subSector) {
    this.subSector = StringUtils.trim(subSector);
  }
  
  public double getIpoPrice() {
    return this.ipoPrice;
  }
  
  public void setIpoPrice(double ipoPrice) {
    this.ipoPrice = ipoPrice;
  }
  
  public double getBasePrice() {
    return this.basePrice;
  }
  
  public void setBasePrice(double basePrice) {
    this.basePrice = basePrice;
  }
  
  public long getListedShares() {
    return this.listedShares;
  }
  
  public void setListedShares(long listedShares) {
    this.listedShares = listedShares;
  }
  
  public long getTradableListedShares() {
    return this.tradableListedShares;
  }
  
  public void setTradableListedShares(long tradableListedShares) {
    this.tradableListedShares = tradableListedShares;
  }
  
  public int getSharesPerLot() {
    return this.sharesPerLot;
  }
  
  public void setSharesPerLot(int sharesPerLot) {
    this.sharesPerLot = sharesPerLot;
  }
  
  public String toString() {
    return "Stock [securityCode=" + this.securityCode + ", securityName=" + this.securityName + ", securityStatus=" + 
      this.securityStatus + ", securityType=" + this.securityType + ", subSector=" + this.subSector + ", ipoPrice=" + 
      this.ipoPrice + ", basePrice=" + this.basePrice + ", listedShares=" + this.listedShares + ", tradableListedShares=" + 
      this.tradableListedShares + ", sharesPerLot=" + this.sharesPerLot + ", remarks=" + this.remarks + 
      ", securityRemarks2=" + this.securityRemarks2 + ", weight=" + this.weight + "]";
  }
  
  public String getRemarks() {
    return this.remarks;
  }
  
  public void setRemarks(String remarks) {
    this.remarks = remarks;
  }
  
  public String getSecurityRemarks2() {
    return this.securityRemarks2;
  }
  
  public void setSecurityRemarks2(String securityRemarks2) {
    this.securityRemarks2 = securityRemarks2;
  }
  
  public double getWeight() {
    return this.weight;
  }
  
  public void setWeight(double weight) {
    this.weight = weight;
  }
  
  public String convertDataStream() {
    return "ANTR|A|" + this.securityCode + "|" + this.securityName + "|" + this.securityStatus + "|" + this.securityType + "|" + this.subSector + "|" + 
      this.ipoPrice + "|" + this.basePrice + "|" + this.listedShares + "|" + this.tradableListedShares + "|" + this.sharesPerLot + "|" + this.remarks + 
      this.securityRemarks2 + "|" + this.weight;
  }
}
