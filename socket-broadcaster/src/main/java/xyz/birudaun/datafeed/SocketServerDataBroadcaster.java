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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import xyz.birudaun.datafeed.db.DbOperations;
import xyz.birudaun.datafeed.db.HsqldbManager;
import xyz.birudaun.datafeed.db.HsqldbOperation;
import xyz.birudaun.datafeed.db.MysqldbOperation;



// Setup websocket server
public class SocketServerDataBroadcaster {
  // Holder for client channels
  private ClientChannelHolder clientChannelHolder = new ClientChannelHolder();

  /**
   * Constructs a new WebSocket server.
   *
   * @throws InterruptedException if interrupted while waiting
   */
  public SocketServerDataBroadcaster() throws InterruptedException {
    // Create an event loop group
    NioEventLoopGroup group = new NioEventLoopGroup();

    // Create a server bootstrap for Netty
    ServerBootstrap bootstrap = new ServerBootstrap();

    // Read port address from environment variable SOCKET_PORT
    int portAddress = Integer.parseInt(System.getenv("SOCKET_PORT"));

    // Configure the server bootstrap
    bootstrap.group(group)
            .channel(NioServerSocketChannel.class) // Use NIO for the server channel
            .localAddress(new InetSocketAddress(portAddress)) // Bind to the specified port
            .childHandler(new ChannelInitializer<SocketChannel>() {
              @Override
              protected void initChannel(SocketChannel ch) throws Exception {
                // Add a handler to the channel pipeline
                ch.pipeline().addLast(new ClientChannelHandler(clientChannelHolder));
              }
            });

    // Bind the server and add a listener
    ChannelFuture future = bootstrap.bind().addListener((ChannelFutureListener) future1 -> {
      if (future1.isSuccess()) {
        System.out.println("Server bound successfully");
      } else {
        System.err.println("Server bind failed");
      }
    });

    // Wait for the client channel holder
    synchronized (clientChannelHolder) {
      clientChannelHolder.wait();
    }
  }

  
  private static final DateFormat DF = new SimpleDateFormat("HH:mm:ss");

  /**
   * Entry point for starting the WebSocket server and data broadcasting.
   *
   * @param args Command line arguments (not used)
   * @throws InterruptedException if interrupted while waiting
   */
  public static void main(String[] args) throws InterruptedException {
    final DateFormat DF = new SimpleDateFormat("HH:mm:ss");

    // Start HSQLDB server
    try {
      HsqldbManager manager = new HsqldbManager();
      manager.startServer();
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    // Create necessary HSQLDB tables
    try {
      HsqldbOperation.createTables();
    } catch (Exception ex) {
      ex.printStackTrace();
    }

    // Initialize WebSocket server
    final SocketServerDataBroadcaster socketServer = new SocketServerDataBroadcaster();

    // Start a timer to execute tasks at fixed intervals
    Timer timer = new Timer();
    // Execute the task at an interval of 15 seconds
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        // Print current timestamp
        System.out.println("Run at " + DF.format(new Date()));

        // Retrieve times without stock summaries
        List<String> timeList = HsqldbOperation.getTimeWithoutStockSummary();

        if (timeList.isEmpty()) {
          // If no times to process, broadcast ping
          System.out.println("\tBroadcasting !PING");
          socketServer.clientChannelHolder.broadCast("!PING");
        }

        // Process each time in the list
        for (String time : timeList) {
          System.out.println("Processing " + time + "...");
          DateTimeFormatter dtf = DateTimeFormatter.ofPattern("HHmm");
          LocalDateTime now = LocalDateTime.now();
          String actualTime = dtf.format(now);

          // Check if the current time has passed
          if (!time.equals(actualTime)) {
            // Perform database operations
            HsqldbOperation.checkpointDefrag();
            System.out.println("\tInserting into HSQLDB...");
            HsqldbOperation.insertIntradayStockSummary(time);
            System.out.println("\tInserting into MySQL...");
            MysqldbOperation.insertIntradayStockSummary(time);

            // Retrieve stock summaries from HSQLDB
            List<String> stockSummaries = HsqldbOperation.getIntradayStockSummary(time);
            StringBuilder sbStockSummary = new StringBuilder();
            for (String stockSummary : stockSummaries) {
              sbStockSummary.append(stockSummary).append("\n");
            }

            // Broadcast the stock summaries
            // eg:
            //  "PTBA,2024/02/26,08:59,2640,2640,2640,2640,37600,0,0,0"
            //  "PGEO,2024/02/26,08:59,1230,1230,1230,1230,133200,0,0,0"
            //  "PGAS,2024/02/26,08:59,1160,1160,1160,1160,354000,0,0,0"
            //
            socketServer.clientChannelHolder.broadCast(sbStockSummary.toString());
            System.out.println("\tBroadcast complete...");
          }
        }

        // Print completion timestamp
        System.out.println("Finished at " + new SimpleDateFormat("HH:mm:ss").format(new Date()));
        System.out.println();
      }
    }, 0L, 15000L); // Initial delay of 0 ms, repeat every 15 seconds
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
    private final SocketServerDataBroadcaster.ClientChannelHolder clientChannelHolder;
    
    public ClientChannelHandler(SocketServerDataBroadcaster.ClientChannelHolder clientChannelHolder) {
      this.clientChannelHolder = clientChannelHolder;
    }
    
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      Channel channel = ctx.channel();
      this.clientChannelHolder.addClientChannel(channel);
      System.out.println("Client connected from " + channel.remoteAddress() + " at " + (
          new SimpleDateFormat("dd-MM-yyyy HH:mm:ss")).format(new Date()));
      super.channelActive(ctx);
      List<String> stockSummaries = HsqldbOperation.getTodayIntradayStockSummary();
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
        } else {
          System.out.println("message: " + strMessage);
        }  
    }
  }
}
