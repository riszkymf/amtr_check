package dev.birudaun.stocksummary.beans;

public class Trade {
  private String tradeDate;
  
  private String tradeTime;
  
  private String securityCode;
  
  private String boardCode;
  
  private String tradeNo;
  
  private double price;
  
  private long volume;
  
  public String getTradeDate() {
    return this.tradeDate;
  }
  
  public void setTradeDate(String tradeDate) {
    this.tradeDate = tradeDate;
  }
  
  public String getTradeTime() {
    return this.tradeTime;
  }
  
  public void setTradeTime(String tradeTime) {
    this.tradeTime = tradeTime;
  }
  
  public String getSecurityCode() {
    return this.securityCode;
  }
  
  public void setSecurityCode(String securityCode) {
    this.securityCode = securityCode;
  }
  
  public String getBoardCode() {
    return this.boardCode;
  }
  
  public void setBoardCode(String boardCode) {
    this.boardCode = boardCode;
  }
  
  public String getTradeNo() {
    return this.tradeNo;
  }
  
  public void setTradeNo(String tradeNo) {
    this.tradeNo = tradeNo;
  }
  
  public double getPrice() {
    return this.price;
  }
  
  public void setPrice(double price) {
    this.price = price;
  }
  
  public long getVolume() {
    return this.volume;
  }
  
  public void setVolume(long volume) {
    this.volume = volume;
  }
  
  public String toString() {
    return "Trade [tradeData=" + this.tradeDate + ", tradeTime=" + this.tradeTime + ", securityCode=" + this.securityCode + 
      ", boardCode=" + this.boardCode + ", tradeNo=" + this.tradeNo + ", price=" + this.price + ", volume=" + this.volume + "]";
  }
}
