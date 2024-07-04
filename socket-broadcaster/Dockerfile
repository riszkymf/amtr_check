FROM openjdk:8
LABEL authors="mfrszk"
COPY target/socket-broadcaster-1.0-jar-with-dependencies.jar app.jar
COPY application.properties application.properties
CMD ["java","-cp","app.jar","xyz.birudaun.datafeed.SocketServerDataBroadcaster"]