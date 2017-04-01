package com.github.lightcopy.fs;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event processing thread to capture HDFS events.
 */
public class EventProcess implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(EventProcess.class);
  // polling interval in milliseconds = 0.25 sec + random interval
  public static final int POLLING_INTERVAL_MS = 250;

  private final HdfsManager manager;
  private volatile boolean stopped;
  private final Random rand;

  public EventProcess(HdfsManager manager) {
    this.manager = manager;
    this.stopped = false;
    this.rand = new Random();
  }

  @Override
  public void run() {
    EventBatch batch = null;
    while (!this.stopped) {
      try {
        while ((batch = this.manager.getEventStream().poll()) != null) {
          long transaction = batch.getTxid();
          LOG.debug("Processing batch transaction {}", transaction);
          for (Event event : batch.getEvents()) {
            long startTime = System.nanoTime();
            processEvent(event, transaction);
            long endTime = System.nanoTime();
            LOG.info("Processed event in {} ms", (endTime - startTime) / 1e6);
          }
        }
        long interval = POLLING_INTERVAL_MS + rand.nextInt(POLLING_INTERVAL_MS);
        LOG.trace("Waiting to poll, interval={}", interval);
        Thread.sleep(interval);
      } catch (Exception err) {
        LOG.error("Thread intrerrupted", err);
        this.stopped = true;
      }
    }
  }

  /** Whether or not event process is stopped */
  public boolean isStopped() {
    return this.stopped;
  }

  /** Mark event process thread as terminated */
  public void terminate() {
    this.stopped = true;
  }

  /**
   * Invoke one of the methods to process specific event, works as dispatcher.
   * Throws exception if event is unsupported or null.
   */
  protected void processEvent(Event event, long transactionId) throws Exception {
    if (event == null) {
      throw new NullPointerException("Event null for transaction " + transactionId);
    }
    switch (event.getEventType()) {
      case APPEND:
        doAppend((Event.AppendEvent) event, transactionId);
        break;
      case CLOSE:
        doClose((Event.CloseEvent) event, transactionId);
        break;
      case CREATE:
        doCreate((Event.CreateEvent) event, transactionId);
        break;
      case METADATA:
        doMetadataUpdate((Event.MetadataUpdateEvent) event, transactionId);
        break;
      case RENAME:
        doRename((Event.RenameEvent) event, transactionId);
        break;
      case UNLINK:
        doUnlink((Event.UnlinkEvent) event, transactionId);
        break;
      default:
        throw new UnsupportedOperationException("Unrecognized event " + event);
    }
  }

  protected void doAppend(Event.AppendEvent event, long transactionId) throws IOException {
    // we do not process append events
    LOG.info("APPEND(path={})", event.getPath());
  }

  protected void doClose(Event.CloseEvent event, long transactionId) {
    LOG.info("CLOSE(filesize={}, path={}, ts={})",
      event.getFileSize(), event.getPath(), event.getTimestamp());
  }

  protected void doCreate(Event.CreateEvent event, long transactionId) throws IOException {
    LOG.info("CREATE(group={}, owner={}, path={})",
      event.getGroupName(), event.getOwnerName(), event.getPath());
  }

  protected void doMetadataUpdate(Event.MetadataUpdateEvent event, long transactionId) {
    LOG.info("METADATA(acls={}, group={}, owner={}, path={}, atime={}, perms={})",
      event.getAcls(), event.getGroupName(), event.getOwnerName(), event.getPath(),
      event.getAtime(), event.getPerms());
  }

  protected void doRename(Event.RenameEvent event, long transactionId) {
    LOG.info("RENAME(ts={}, src={}, dst={})",
      event.getTimestamp(), event.getSrcPath(), event.getDstPath());
  }

  protected void doUnlink(Event.UnlinkEvent event, long transactionId) {
    LOG.info("UNLINK(ts={}, path={})", event.getTimestamp(), event.getPath());
  }
}
