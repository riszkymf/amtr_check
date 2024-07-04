package dev.birudaun.antr.beans;

public class BidOffer {
  private double price;
  
  private long volume;
  
  private int queue;
  
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
  
  public int getQueue() {
    return this.queue;
  }
  
  public void setQueue(int queue) {
    this.queue = queue;
  }
  
  public String toString() {
    return "BidOffer [price=" + this.price + ", volume=" + this.volume + ", queue=" + this.queue + "]";
  }
}
