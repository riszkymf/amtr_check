package xyz.birudaun.datafeed;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.net.InetSocketAddress;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import xyz.birudaun.datafeed.db.DbOperations;

public class MysqlSocketServerDataBroadcaster {
  private ClientChannelHolder clientChannelHolder = new ClientChannelHolder();
  
  public MysqlSocketServerDataBroadcaster() throws InterruptedException {
    NioEventLoopGroup group = new NioEventLoopGroup();
    ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(group)
            .channel(NioServerSocketChannel.class)
            .childHandler(new ChannelInitializer<SocketChannel>(){
              protected void initChannel(SocketChannel ch) throws Exception{
                ch.pipeline().addLast(
                        new MysqlSocketServerDataBroadcaster.ClientChannelHandler(MysqlSocketServerDataBroadcaster.this.clientChannelHolder)
                );
              };
            });

    ChannelFuture future = bootstrap.bind().addListener((GenericFutureListener)new ChannelFutureListener() {
          public void operationComplete(ChannelFuture future) throws Exception {
            System.out.println("Server bound ");
          }
        });
    synchronized (this.clientChannelHolder) {
      this.clientChannelHolder.wait();
    } 
  }
  
  public static class ClientChannelHolder {
    private List<Channel> clientChannels = new LinkedList<>();
    
    private synchronized void addClientChannel(Channel socketChannel) {
      notify();
      this.clientChannels.add(socketChannel);
    }
    
    private synchronized void removeClientChannel(Channel socketChannel) {
      this.clientChannels.remove(socketChannel);
    }
    
    public void broadCast(String content) {
      int cntClient = 0;
      for (Channel channel : this.clientChannels) {
        System.out.println(++cntClient);
        byte[] bytes = content
          .getBytes(CharsetUtil.UTF_8);
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(bytes);
        channel.writeAndFlush(byteBuf).addListener(new GenericFutureListener<Future<? super Void>>() {
              public void operationComplete(Future<? super Void> future) throws Exception {
                if (future.isSuccess())
                  System.out.println("Write successfull"); 
              }
            });
      } 
    }
  }
  
  public static class ClientChannelHandler extends ChannelInboundHandlerAdapter {
    private final MysqlSocketServerDataBroadcaster.ClientChannelHolder clientChannelHolder;
    
    public ClientChannelHandler(MysqlSocketServerDataBroadcaster.ClientChannelHolder clientChannelHolder) {
      this.clientChannelHolder = clientChannelHolder;
    }
    
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      Channel channel = ctx.channel();
      this.clientChannelHolder.addClientChannel(channel);
      System.out.println("Client connected from " + channel.remoteAddress() + " at " + (
          new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")).format(new Date()));
      super.channelActive(ctx);
      List<String> stockSummaries = DbOperations.getIdxIntradayStockSummaryLast3Days();
      StringBuffer sbStockSummary = new StringBuffer();
      for (String stockSummary : stockSummaries)
        sbStockSummary.append(stockSummary).append("\n"); 
      String content = new String(sbStockSummary);
      byte[] bytes = content.getBytes(CharsetUtil.UTF_8);
      ByteBuf byteBuf = Unpooled.buffer();
      byteBuf.writeBytes(bytes);
      channel.writeAndFlush(byteBuf).addListener(new GenericFutureListener<Future<? super Void>>() {
            public void operationComplete(Future<? super Void> future) throws Exception {
              if (future.isSuccess())
                System.out.println("Welcome message write successfully"); 
            }
          });
    }
    
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      this.clientChannelHolder.removeClientChannel(ctx.channel());
      super.channelInactive(ctx);
    }
    
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      ByteBuf in = (ByteBuf)msg;
      String strMessage = in.toString(CharsetUtil.UTF_8);
      System.out.println("channelRead: " + strMessage);
      if (strMessage != null)
        if ("stockinfo".equals(strMessage.trim().toLowerCase())) {
          System.out.println("\tmasuk ke stockinfo");
          List<String> stockInfos = DbOperations.getIdxStockIndustryClassification();
          StringBuffer sbStockInfo = new StringBuffer();
          for (String stockInfo : stockInfos) {
            System.out.println(stockInfo);
            sbStockInfo.append(stockInfo).append("\n");
          } 
          String content = new String(sbStockInfo);
          System.out.println("\t" + content);
          byte[] bytes = content.getBytes(CharsetUtil.UTF_8);
          ByteBuf byteBuf = Unpooled.buffer();
          byteBuf.writeBytes(bytes);
          ctx.channel().writeAndFlush(byteBuf).addListener(new GenericFutureListener<Future<? super Void>>() {
                public void operationComplete(Future<? super Void> future) throws Exception {
                  if (future.isSuccess())
                    System.out.println("Successfully sent stock info..."); 
                }
              });
        } else if ("backfill".equals(strMessage.trim().toLowerCase())) {
          List<String> stockSummaries = DbOperations.getIdxIntradayStockSummaryBackfill();
          StringBuffer sbStockSummary = new StringBuffer();
          for (String stockSummary : stockSummaries)
            sbStockSummary.append(stockSummary).append("\n"); 
          String content = new String(sbStockSummary);
          byte[] bytes = content.getBytes(CharsetUtil.UTF_8);
          ByteBuf byteBuf = Unpooled.buffer();
          byteBuf.writeBytes(bytes);
          ctx.channel().writeAndFlush(byteBuf).addListener(new GenericFutureListener<Future<? super Void>>() {
                public void operationComplete(Future<? super Void> future) throws Exception {
                  if (future.isSuccess())
                    System.out.println("Successfully sent backfill stock summary data..."); 
                }
              });
        }  
    }
  }
}
