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

import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;

import com.github.lightcopy.conf.AppConf;

/**
 * Implementation of [[FileSystemManager]] for HDFS.
 */
public class HdfsManager extends FileSystemManager {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsManager.class);

  public static final String MONGO_DATABASE = "dbfs_db";
  public static final String MONGO_COLLECTION_FILE_SYSTEM = "file_system";
  public static final String MONGO_COLLECTION_EVENT_POOL = "event_pool";

  private HdfsAdmin admin;
  private FileSystem fs;
  private MongoClient mongo;
  private Path root;
  private DFSInotifyEventInputStream eventStream;
  private EventProcess eventProcess;
  private Thread eventProcessThread;

  public HdfsManager(AppConf conf) {
    this(conf.hdfsURI(), new Path("/"), conf.mongoConnectionString());
  }

  public HdfsManager(URI hdfsURI, Path root, String mongoConnection) {
    try {
      LOG.info("Initialize hdfs manager with uri {}", hdfsURI);
      Configuration hadoopConfiguration = new Configuration(false);
      this.admin = new HdfsAdmin(hdfsURI, hadoopConfiguration);
      this.eventStream = this.admin.getInotifyEventStream();
      LOG.info("Initialize file system for uri {}", hdfsURI);
      this.fs = FileSystem.get(hdfsURI, hadoopConfiguration);
      LOG.info("Initialize mongo client for connection {}", mongoConnection);
      this.mongo = new MongoClient(new MongoClientURI(mongoConnection));
      LOG.info("Set root path as {}", root);
      this.root = root;
    } catch (IOException ioe) {
      String msg = "Failed to initialize hdfs manager";
      LOG.error(msg, ioe);
      throw new RuntimeException(msg, ioe);
    }
  }

  /** Get mongo collection for file system metadata */
  protected MongoCollection<Document> mongoFileSystem() {
    return this.mongo.getDatabase(MONGO_DATABASE).getCollection(MONGO_COLLECTION_FILE_SYSTEM);
  }

  /** Get mongo collection to store hdfs events */
  protected MongoCollection<Document> mongoEventPool() {
    return this.mongo.getDatabase(MONGO_DATABASE).getCollection(MONGO_COLLECTION_EVENT_POOL);
  }

  private void cleanupState() {
    // delete database if exists
    LOG.info("Delete Mongo database {}", MONGO_DATABASE);
    this.mongo.getDatabase(MONGO_DATABASE).drop();
  }

  private void startEventProcessing() {
    this.eventProcess = new EventProcess(this);
    this.eventProcessThread = new Thread(this.eventProcess);
    LOG.info("Start event processing ({})", this.eventProcessThread);
    this.eventProcessThread.start();
  }

  private void stopEventProcessing() {
    LOG.info("Stop event processing ({})", this.eventProcessThread);
    if (this.eventProcessThread != null) {
      try {
        this.eventProcess.terminate();
        this.eventProcessThread.join();
        // reset properties to null
        this.eventProcessThread = null;
        this.eventProcess = null;
      } catch (InterruptedException err) {
        throw new RuntimeException("Intrerrupted thread " + this.eventProcessThread, err);
      }
    }
  }

  protected DFSInotifyEventInputStream getEventStream() {
    return this.eventStream;
  }

  @Override
  public TreeVisitor prepareTreeVisitor() {
    return new NodeTreeVisitor(mongoFileSystem());
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
    LOG.info("Started in {} ms", (endTime - startTime) / 1e6);
  }

  @Override
  public void stop() {
    long startTime = System.nanoTime();
    LOG.info("Stop hdfs manager");
    stopEventProcessing();
    // it does not seem like you can close event stream
    this.eventStream = null;
    // reset hdfs admin
    this.admin = null;
    // close mongodb
    this.mongo.close();
    this.mongo = null;
    long endTime = System.nanoTime();
    LOG.info("Stopped in {} ms", (endTime - startTime) / 1e6);
  }
}
