package com.github.lightcopy;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Properties;

import javax.ws.rs.core.UriBuilder;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lightcopy.conf.AppConf;
import com.github.lightcopy.fs.HdfsManager;

/**
 * Main entrypoint to launch web server. Provides basic application context for providers.
 * Providers must be registered manually without web.xml.
 */
public class WebServer {
  private static Logger LOG = LoggerFactory.getLogger(WebServer.class);

  private final String scheme;
  private final String host;
  private final int port;
  private final HttpServer server;
  private ArrayList<Runnable> events;
  private AppConf conf;
  private HdfsManager manager;

  /**
   * Application context.
   * Only manual lookup is enabled, provider discovery is disabled.
   */
  static class ApplicationContext extends ResourceConfig {
    public ApplicationContext() {
      register(ContextProvider.class);
      property(ServerProperties.METAINF_SERVICES_LOOKUP_DISABLE, true);
    }
  }

  /**
   * Shutdown hook to gracefully stop web server.
   */
  static class ServerShutdown implements Runnable {
    private final WebServer server;

    ServerShutdown(WebServer server) {
      this.server = server;
    }

    @Override
    public void run() {
      this.server.shutdown();
    }

    @Override
    public String toString() {
      return "ServerShutdown" + this.server;
    }
  }

  /**
   * Shutdown hook to HdfsManager.
   */
  static class HdfsManagerShutdown implements Runnable {
    private final HdfsManager manager;

    HdfsManagerShutdown(HdfsManager manager) {
      this.manager = manager;
    }

    @Override
    public void run() {
      if (this.manager != null) {
        this.manager.stop();
      }
    }

    @Override
    public String toString() {
      return "HdfsManagerShutdown" + this.manager;
    }
  }

  public WebServer(Properties props) {
    this.conf = new AppConf(props);
    this.scheme = this.conf.scheme();
    this.host = this.conf.httpHost();
    this.port = this.conf.httpPort();
    // initialize events list and internal server
    this.events = new ArrayList<Runnable>();
    this.server = createHttpServer();
    this.manager = new HdfsManager(this.conf);
    registerShutdownHook(new HdfsManagerShutdown(this.manager));
  }

  /** Create endpoint uri from initialized properties */
  protected URI createEndpoint() {
    return UriBuilder.fromPath("")
      .scheme(this.scheme)
      .host(this.host)
      .port(this.port)
      .build();
  }

  /** Create http server from initialized properties */
  protected HttpServer createHttpServer() {
    URI endpoint = createEndpoint();
    ApplicationContext context = new ApplicationContext();
    return GrizzlyHttpServerFactory.createHttpServer(endpoint, context);
  }

  /**
   * Shutdown server. This should not be used directly, instead call launch() method. It will
   * add shutdown hook to gracefully stop server.
   */
  protected void shutdown() {
    LOG.info("Stop server {}", this);
    this.server.shutdown();
  }

  /** Get current host */
  public String getHost() {
    return this.host;
  }

  /** Get current port */
  public int getPort() {
    return this.port;
  }

  /**
   * Register shutdown hook to call when server is about to be stopped. These events are always
   * called before server shutdown.
   */
  public void registerShutdownHook(Runnable event) {
    this.events.add(event);
  }

  /**
   * Start web server using provided options. As part of initialization registers all shutdown
   * hooks, including one for the server.
   */
  public void launch() throws IOException {
    // register shutdown hook for server after all events
    registerShutdownHook(new ServerShutdown(this));
    for (Runnable event : this.events) {
      LOG.info("Register shutdown event {}", event);
      Runtime.getRuntime().addShutdownHook(new Thread(event));
    }
    LOG.info("Start server {}", this);
    this.server.start();
    this.manager.start();
  }

  @Override
  public String toString() {
    return "[" + createEndpoint() + "]";
  }

  public static void main(String[] args) {
    try {
      LOG.info("Initialize web server");
      WebServer server = new WebServer(System.getProperties());
      LOG.info("Created web server {}", server);
      server.launch();
    } catch (Exception err) {
      LOG.error("Exception occuried", err);
      System.exit(1);
    }
  }
}
