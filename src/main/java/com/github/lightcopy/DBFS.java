package com.github.lightcopy;

import com.github.lightcopy.fs.HdfsManager;

/**
 * Main entrypoint to launch dbfs web server. Provides basic application context for providers.
 * Providers must be registered manually without web.xml.
 */
public class DBFS extends Server {
  private static final int PING_INTERVAL = 2000;

  private HdfsManager manager;

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

  public DBFS() {
    super();
    this.manager = new HdfsManager(this.conf);
    registerShutdownHook(new HdfsManagerShutdown(this.manager));
  }

  @Override
  public void afterLaunch() {
    this.manager.start();
    try {
      while (true) {
        if (!this.manager.status()) {
          Thread.currentThread().interrupt();
        }
        LOG.trace("Ping server for status...");
        Thread.sleep(PING_INTERVAL);
      }
    } catch (InterruptedException err) {
      throw new RuntimeException(err);
    }
  }

  public static void main(String[] args) {
    try {
      LOG.info("Initialize web server");
      DBFS server = new DBFS();
      LOG.info("Created dbfs {}", server);
      server.launch();
    } catch (Exception err) {
      LOG.error("Exception occurred", err);
      System.exit(1);
    }
  }
}
