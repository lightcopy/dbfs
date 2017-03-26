package com.github.lightcopy;

import java.net.URI;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;

public class WebServer {
  public static void main(String[] args) {
    try {
      String scheme = "http";
      String host = "0.0.0.0";
      int port = 8080;
      URI endpoint = UriBuilder.fromPath("")
        .scheme(scheme)
        .host(host)
        .port(port)
        .build();
      System.out.println("Created endpoint: " + endpoint);
      ApplicationContext appContext = new ApplicationContext();
      final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(endpoint, appContext);
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          System.out.println("Shutdown server");
          server.shutdown();
        }
      });
      server.start();
    } catch (Exception err) {
      System.out.println("Exception: " + err);
    }
  }
}
