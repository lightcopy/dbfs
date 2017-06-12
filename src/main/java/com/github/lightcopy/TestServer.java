package com.github.lightcopy;

/** Simple development server for frontend */
public class TestServer extends Server {

  public TestServer() {
    super();
  }

  public static void main(String[] args) {
    try {
      LOG.info("Initialize web server");
      TestServer server = new TestServer();
      LOG.info("Created server {}", server);
      server.launch();
    } catch (Exception err) {
      LOG.error("Exception occurred", err);
      System.exit(1);
    }
  }
}
