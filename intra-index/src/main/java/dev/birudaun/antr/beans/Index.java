package dev.birudaun.antr.beans;

public class Index {
  private String indexCode;
  
  private String date;
  
  private String time;
  
  private double lastIndex;
  
  private double prevIndex;
  
  private double openIndex;
  
  private double highIndex;
  
  private double lowIndex;
  
  private double change;
  
  private double changeRate;
  
  private long freq;
  
  private long volume;
  
  private double value;
  
  private int up;
  
  private int down;
  
  private int unchange;
  
  private int noTransaction;
  
  private double baseValue;
  
  private double marketValue;
  
  private long fgBuyFreq;
  
  private long fgSellFreq;
  
  private long fgBuyVolume;
  
  private long fgSellVolume;
  
  private double fgBuyValue;
  
  private double fgSellValue;
  
  public String getDate() {
    return this.date;
  }
  
  public void setDate(String date) {
    this.date = date;
  }
  
  public void setIndexCode(String indexCode) {
    this.indexCode = indexCode;
  }
  
  public void setTime(String time) {
    this.time = time;
  }
  
  public void setLastIndex(double lastIndex) {
    this.lastIndex = lastIndex;
  }
  
  public void setPrevIndex(double prevIndex) {
    this.prevIndex = prevIndex;
  }
  
  public void setOpenIndex(double openIndex) {
    this.openIndex = openIndex;
  }
  
  public void setHighIndex(double highIndex) {
    this.highIndex = highIndex;
  }
  
  public void setLowIndex(double lowIndex) {
    this.lowIndex = lowIndex;
  }
  
  public void setChange(double change) {
    this.change = change;
  }
  
  public void setChangeRate(double changeRate) {
    this.changeRate = changeRate;
  }
  
  public void setFreq(long freq) {
    this.freq = freq;
  }
  
  public void setVolume(long volume) {
    this.volume = volume;
  }
  
  public void setValue(double value) {
    this.value = value;
  }
  
  public void setUp(int up) {
    this.up = up;
  }
  
  public void setDown(int down) {
    this.down = down;
  }
  
  public void setUnchange(int unchange) {
    this.unchange = unchange;
  }
  
  public void setNoTransaction(int noTransaction) {
    this.noTransaction = noTransaction;
  }
  
  public void setBaseValue(double baseValue) {
    this.baseValue = baseValue;
  }
  
  public void setMarketValue(double marketValue) {
    this.marketValue = marketValue;
  }
  
  public void setFgBuyFreq(long fgBuyFreq) {
    this.fgBuyFreq = fgBuyFreq;
  }
  
  public void setFgSellFreq(long fgSellFreq) {
    this.fgSellFreq = fgSellFreq;
  }
  
  public void setFgBuyVolume(long fgBuyVolume) {
    this.fgBuyVolume = fgBuyVolume;
  }
  
  public void setFgSellVolume(long fgSellVolume) {
    this.fgSellVolume = fgSellVolume;
  }
  
  public void setFgBuyValue(double fgBuyValue) {
    this.fgBuyValue = fgBuyValue;
  }
  
  public void setFgSellValue(double fgSellValue) {
    this.fgSellValue = fgSellValue;
  }
  
  public String getIndexCode() {
    return this.indexCode;
  }
  
  public String getTime() {
    return this.time;
  }
  
  public double getLastIndex() {
    return this.lastIndex / 1000.0D;
  }
  
  public double getPrevIndex() {
    return this.prevIndex / 1000.0D;
  }
  
  public double getOpenIndex() {
    return this.openIndex / 1000.0D;
  }
  
  public double getHighIndex() {
    return this.highIndex / 1000.0D;
  }
  
  public double getLowIndex() {
    return this.lowIndex / 1000.0D;
  }
  
  public double getChange() {
    return this.change;
  }
  
  public double getChangeRate() {
    return this.changeRate;
  }
  
  public long getFreq() {
    return this.freq;
  }
  
  public long getVolume() {
    return this.volume;
  }
  
  public double getValue() {
    return this.value;
  }
  
  public int getUp() {
    return this.up;
  }
  
  public int getDown() {
    return this.down;
  }
  
  public int getUnchange() {
    return this.unchange;
  }
  
  public int getNoTransaction() {
    return this.noTransaction;
  }
  
  public double getBaseValue() {
    return this.baseValue;
  }
  
  public double getMarketValue() {
    return this.marketValue;
  }
  
  public long getFgBuyFreq() {
    return this.fgBuyFreq;
  }
  
  public long getFgSellFreq() {
    return this.fgSellFreq;
  }
  
  public long getFgBuyVolume() {
    return this.fgBuyVolume;
  }
  
  public long getFgSellVolume() {
    return this.fgSellVolume;
  }
  
  public double getFgBuyValue() {
    return this.fgBuyValue;
  }
  
  public double getFgSellValue() {
    return this.fgSellValue;
  }
  
  public String toString() {
    return "Index [indexCode=" + this.indexCode + ", time=" + this.time + ", lastIndex=" + this.lastIndex + ", prevIndex=" + 
      this.prevIndex + ", openIndex=" + this.openIndex + ", highIndex=" + this.highIndex + ", lowIndex=" + this.lowIndex + 
      ", change=" + this.change + ", changeRate=" + this.changeRate + ", freq=" + this.freq + ", volume=" + this.volume + 
      ", value=" + this.value + ", up=" + this.up + ", down=" + this.down + ", unchange=" + this.unchange + ", noTransaction=" + 
      this.noTransaction + ", baseValue=" + this.baseValue + ", marketValue=" + this.marketValue + ", fgBuyFreq=" + 
      this.fgBuyFreq + ", fgSellFreq=" + this.fgSellFreq + ", fgBuyVolume=" + this.fgBuyVolume + ", fgSellVolume=" + 
      this.fgSellVolume + ", fgBuyValue=" + this.fgBuyValue + ", fgSellValue=" + this.fgSellValue + "]";
  }
  
  public String getPublishData(String dataDate) {
    return "ANTR|9|" + this.indexCode + "|" + dataDate + "|" + this.time + "|" + this.lastIndex + "|" + this.prevIndex + "|" + this.openIndex + 
      "|" + this.highIndex + "|" + this.lowIndex + "|" + this.change + "|" + this.changeRate + "|" + this.freq + "|" + this.volume + 
      "|" + this.value + "|" + this.up + "|" + this.down + "|" + this.unchange + "|" + this.noTransaction + "|" + this.marketValue;
  }
}
