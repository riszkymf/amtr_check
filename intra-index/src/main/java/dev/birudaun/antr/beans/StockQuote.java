package dev.birudaun.antr.beans;

public class StockQuote {
  private String securityCode;
  
  private String boardCode;
  
  private String date;
  
  private String time;
  
  private double prevPrice;
  
  private double prevChg;
  
  private double prevChgRate;
  
  private double closePrice;
  
  private double change;
  
  private double changeRate;
  
  private double openPrice;
  
  private double highPrice;
  
  private double lowPrice;
  
  private double avgPrice;
  
  private long freq;
  
  private long volume;
  
  private long value;
  
  private long marketCap;
  
  private double bestBidPrice;
  
  private long bestBidVolume;
  
  private double bestOfferPrice;
  
  private long bestOfferVolume;
  
  public String getDate() {
    return this.date;
  }
  
  public void setDate(String date) {
    this.date = date;
  }
  
  public String getSecurityCode() {
    return this.securityCode;
  }
  
  public void setSecurityCode(String stockCode) {
    this.securityCode = stockCode;
  }
  
  public String getBoardCode() {
    return this.boardCode;
  }
  
  public void setBoardCode(String board) {
    this.boardCode = board;
  }
  
  public String getTime() {
    return this.time;
  }
  
  public void setTime(String time) {
    this.time = time;
  }
  
  public double getPrevPrice() {
    return this.prevPrice;
  }
  
  public void setPrevPrice(double prevPrice) {
    this.prevPrice = prevPrice;
  }
  
  public double getPrevChg() {
    return this.prevChg;
  }
  
  public void setPrevChg(double prevChg) {
    this.prevChg = prevChg;
  }
  
  public double getPrevChgRate() {
    return this.prevChgRate;
  }
  
  public void setPrevChgRate(double prevChgRate) {
    this.prevChgRate = prevChgRate;
  }
  
  public double getClosePrice() {
    return this.closePrice;
  }
  
  public void setClosePrice(double lastPrice) {
    this.closePrice = lastPrice;
  }
  
  public double getChange() {
    return this.change;
  }
  
  public void setChange(double change) {
    this.change = change;
  }
  
  public double getChangeRate() {
    return this.changeRate;
  }
  
  public void setChangeRate(double changeRate) {
    this.changeRate = changeRate;
  }
  
  public double getOpenPrice() {
    return this.openPrice;
  }
  
  public void setOpenPrice(double openPrice) {
    this.openPrice = openPrice;
  }
  
  public double getHighPrice() {
    return this.highPrice;
  }
  
  public void setHighPrice(double highPrice) {
    this.highPrice = highPrice;
  }
  
  public double getLowPrice() {
    return this.lowPrice;
  }
  
  public void setLowPrice(double lowPrice) {
    this.lowPrice = lowPrice;
  }
  
  public double getAvgPrice() {
    return this.avgPrice;
  }
  
  public void setAvgPrice(double avgPrice) {
    this.avgPrice = avgPrice;
  }
  
  public long getFreq() {
    return this.freq;
  }
  
  public void setFreq(long freq) {
    this.freq = freq;
  }
  
  public long getVolume() {
    return this.volume;
  }
  
  public void setVolume(long volume) {
    this.volume = volume;
  }
  
  public long getValue() {
    return this.value;
  }
  
  public void setValue(long value) {
    this.value = value;
  }
  
  public long getMarketCap() {
    return this.marketCap;
  }
  
  public void setMarketCap(long marketCap) {
    this.marketCap = marketCap;
  }
  
  public double getBestBidPrice() {
    return this.bestBidPrice;
  }
  
  public void setBestBidPrice(double bestBidPrice) {
    this.bestBidPrice = bestBidPrice;
  }
  
  public long getBestBidVolume() {
    return this.bestBidVolume;
  }
  
  public void setBestBidVolume(long bestBidVolume) {
    this.bestBidVolume = bestBidVolume;
  }
  
  public double getBestOfferPrice() {
    return this.bestOfferPrice;
  }
  
  public void setBestOfferPrice(double bestOfferPrice) {
    this.bestOfferPrice = bestOfferPrice;
  }
  
  public long getBestOfferVolume() {
    return this.bestOfferVolume;
  }
  
  public void setBestOfferVolume(long bestOfferVolume) {
    this.bestOfferVolume = bestOfferVolume;
  }
  
  public String toString() {
    return "StockQuote [stockCode=" + this.securityCode + ", board=" + this.boardCode + ", time=" + this.time + ", prevPrice=" + this.prevPrice + 
      ", prevChg=" + this.prevChg + ", prevChgRate=" + this.prevChgRate + ", lastPrice=" + this.closePrice + ", change=" + 
      this.change + ", changeRate=" + this.changeRate + ", openPrice=" + this.openPrice + ", highPrice=" + this.highPrice + 
      ", lowPrice=" + this.lowPrice + ", avgPrice=" + this.avgPrice + ", freq=" + this.freq + ", volume=" + this.volume + 
      ", value=" + this.value + ", marketCap=" + this.marketCap + ", bestBidPrice=" + this.bestBidPrice + 
      ", bestBidVolume=" + this.bestBidVolume + ", bestOfferPrice=" + this.bestOfferPrice + ", bestOfferVolume=" + 
      this.bestOfferVolume + "]";
  }
  
  public String getPublishData(String dataDate) {
    return "ANTR|2|" + dataDate + "|" + this.time + "|" + this.securityCode + "|" + this.boardCode + "|" + this.openPrice + "|" + this.highPrice + "|" + this.lowPrice + 
      "|" + this.closePrice + "|" + this.volume + "|" + this.value + "|" + this.freq;
  }
}
