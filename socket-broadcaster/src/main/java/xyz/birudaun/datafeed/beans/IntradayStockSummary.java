package xyz.birudaun.datafeed.beans;

public class IntradayStockSummary {
  private String tradeDate;
  
  private String tradeTime;
  
  private String securityCode;
  
  private String boardCode;
  
  private double openingPrice;
  
  private double highestPrice;
  
  private double lowestPrice;
  
  private double closingPrice;
  
  private long tradedVolume;
  
  private double tradedValue;
  
  private long tradedFrequency;
  
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
  
  public double getOpeningPrice() {
    return this.openingPrice;
  }
  
  public void setOpeningPrice(double openingPrice) {
    this.openingPrice = openingPrice;
  }
  
  public double getHighestPrice() {
    return this.highestPrice;
  }
  
  public void setHighestPrice(double highestPrice) {
    this.highestPrice = highestPrice;
  }
  
  public double getLowestPrice() {
    return this.lowestPrice;
  }
  
  public void setLowestPrice(double lowestPrice) {
    this.lowestPrice = lowestPrice;
  }
  
  public double getClosingPrice() {
    return this.closingPrice;
  }
  
  public void setClosingPrice(double closingPrice) {
    this.closingPrice = closingPrice;
  }
  
  public long getTradedVolume() {
    return this.tradedVolume;
  }
  
  public void setTradedVolume(long tradedVolume) {
    this.tradedVolume = tradedVolume;
  }
  
  public double getTradedValue() {
    return this.tradedValue;
  }
  
  public void setTradedValue(double tradedValue) {
    this.tradedValue = tradedValue;
  }
  
  public long getTradedFrequency() {
    return this.tradedFrequency;
  }
  
  public void setTradedFrequency(long tradedFrequency) {
    this.tradedFrequency = tradedFrequency;
  }
  
  public String toString() {
    return "IntradayStockSummary [tradeDate=" + this.tradeDate + ", tradeTime=" + this.tradeTime + ", securityCode=" + 
      this.securityCode + ", boardCode=" + this.boardCode + ", openingPrice=" + this.openingPrice + ", highestPrice=" + 
      this.highestPrice + ", lowestPrice=" + this.lowestPrice + ", closingPrice=" + this.closingPrice + ", tradedVolume=" + 
      this.tradedVolume + ", tradedValue=" + this.tradedValue + ", tradedFrequency=" + this.tradedFrequency + "]";
  }
}
