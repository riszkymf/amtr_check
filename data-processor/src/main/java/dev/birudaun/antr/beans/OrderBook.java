package dev.birudaun.antr.beans;

import java.util.ArrayList;
import java.util.List;

public class OrderBook {
  private String stockCode;
  
  private String board;
  
  private String date;
  
  private String bidData = "";
  
  private String offerData = "";
  
  private int bidArray;
  
  private int offerArray;
  
  private List<BidOffer> bidList = new ArrayList<>();
  
  private List<BidOffer> offerList = new ArrayList<>();
  
  public String getDate() {
    return this.date;
  }
  
  public void setDate(String date) {
    this.date = date;
  }
  
  public String getBidData() {
    return this.bidData;
  }
  
  public void setBidData(String bidData) {
    this.bidData = bidData;
  }
  
  public String getOfferData() {
    return this.offerData;
  }
  
  public void setOfferData(String offerData) {
    this.offerData = offerData;
  }
  
  public String getStockCode() {
    return this.stockCode;
  }
  
  public void setStockCode(String stockCode) {
    this.stockCode = stockCode;
  }
  
  public String getBoard() {
    if (this.board == null)
      this.board = "RG"; 
    return this.board;
  }
  
  public void setBoard(String board) {
    this.board = board;
  }
  
  public int getBidArray() {
    return this.bidArray;
  }
  
  public void setBidArray(int bidArray) {
    this.bidArray = bidArray;
  }
  
  public int getOfferArray() {
    return this.offerArray;
  }
  
  public void setOfferArray(int offerArray) {
    this.offerArray = offerArray;
  }
  
  public List<BidOffer> getBidList() {
    return this.bidList;
  }
  
  public void setBidList(List<BidOffer> bidList) {
    this.bidList = bidList;
  }
  
  public List<BidOffer> getOfferList() {
    return this.offerList;
  }
  
  public void setOfferList(List<BidOffer> offerList) {
    this.offerList = offerList;
  }
  
  public String toString() {
    return "OrderBook [stockCode=" + this.stockCode + ", board=" + this.board + ", date=" + this.date + ", bidData=" + this.bidData + 
      ", offerData=" + this.offerData + ", bidArray=" + this.bidArray + ", offerArray=" + this.offerArray + ", bidList=" + 
      this.bidList + ", offerList=" + this.offerList + "]";
  }
  
  public String getPublishDataBid(String dataDate) {
    return "ANTR|3|" + this.stockCode + "|{0}" + this.bidData;
  }
  
  public String getPublishDataOffer(String dataDate) {
    return "ANTR|4|" + this.stockCode + "|{1}" + this.offerData;
  }
}
