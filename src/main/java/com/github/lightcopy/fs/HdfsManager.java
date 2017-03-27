package com.github.lightcopy.fs;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.lightcopy.conf.AppConf;

/**
 * Implementation of [[FileSystemManager]] for HDFS.
 */
public class HdfsManager extends FileSystemManager {
  private static Logger LOG = LoggerFactory.getLogger(HdfsManager.class);

  private HdfsAdmin admin;
  private FileSystem fs;
  private Path root;
  private DFSInotifyEventInputStream eventStream;
  private Thread eventProcessingThread;

  public HdfsManager(AppConf conf) {
    this(conf.hdfsURI(), new Path("/"));
  }

  public HdfsManager(URI hdfsURI, Path root) {
    try {
      LOG.info("Initialize hdfs manager with uri {}", hdfsURI);
      Configuration hadoopConfiguration = new Configuration(false);
      this.admin = new HdfsAdmin(hdfsURI, hadoopConfiguration);
      this.fs = FileSystem.get(hdfsURI, hadoopConfiguration);
      this.root = root;
    } catch (IOException ioe) {
      String msg = "Failed to initialize hdfs manager";
      LOG.error(msg, ioe);
      throw new RuntimeException(msg, ioe);
    }
  }

  @Override
  public TreeVisitor prepareTreeVisitor() {
    // TODO: implement tree visitor
    return null;
  }

  @Override
  public FileSystem getFileSystem() {
    return this.fs;
  }

  @Override
  public Path getRoot() {
    return this.root;
  }

  @Override
  public void start() {
    long startTime = System.nanoTime();
    LOG.info("Start hdfs manager");
    try {
      this.eventStream = this.admin.getInotifyEventStream();
      // cleanup state
      LOG.info("Clean up current state");
      cleanupState();
      // traverse and reindex file system
      LOG.info("Index file system");
      indexFileSystem();
      // start thread to process events
      LOG.info("Start processing thread");
      startEventProcessing();
    } catch (IOException ioe) {
      String msg = "Failed to start hdfs manager";
      LOG.error(msg, ioe);
      throw new RuntimeException(msg, ioe);
    }
    long endTime = System.nanoTime();
    LOG.info("Started in {} ms", (endTime - startTime) / 1e-6);
  }

  @Override
  public void stop() {
    long startTime = System.nanoTime();
    LOG.info("Stop hdfs manager");
    stopEventProcessing();
    // it does not seem like you can close event stream
    this.eventStream = null;
    long endTime = System.nanoTime();
    LOG.info("Stopped in {} ms", (endTime - startTime) / 1e-6);
  }

  private void cleanupState() {
    // TODO: implement cleanup state
  }

  private void startEventProcessing() {
    // TODO: implement start of the processing thread
  }

  private void stopEventProcessing() {
    // TODO: implement shutdown of the processing thread
  }
}
