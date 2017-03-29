package com.github.lightcopy.fs;

import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Event processing thread to capture HDFS events.
 */
public class EventProcess implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(EventProcess.class);
  // polling interval in milliseconds = 0.5 sec
  public static final long POLLING_INTERVAL = 500;

  private final HdfsManager manager;
  private volatile boolean stopped;

  public EventProcess(HdfsManager manager) {
    this.manager = manager;
    this.stopped = false;
  }

  @Override
  public void run() {
    while (!this.stopped) {
      try {
        EventBatch batch = this.manager.getEventStream().poll();
        if (batch != null) {
          long transaction = batch.getTxid();
          LOG.debug("Processing batch transaction {}", transaction);
          for (Event event : batch.getEvents()) {
            processEvent(event);
          }
        } else {
          LOG.trace("Waiting to poll, time={}", System.currentTimeMillis());
          Thread.sleep(POLLING_INTERVAL);
        }
      } catch (Exception err) {
        LOG.error("Thread intrerrupted", err);
        this.stopped = true;
      }
    }
  }

  public void terminate() {
    this.stopped = true;
  }

  /**
   * Invoke one of the methods to process specific event, works as dispatcher.
   * Throws exception if event is unsupported or null.
   */
  protected void processEvent(Event event) {
    if (event == null) throw new NullPointerException("Event null");

    if (event instanceof Event.AppendEvent) {
      doAppend((Event.AppendEvent) event);
    } else if (event instanceof Event.CloseEvent) {
      doClose((Event.CloseEvent) event);
    } else if (event instanceof Event.CreateEvent) {
      doCreate((Event.CreateEvent) event);
    } else if (event instanceof Event.MetadataUpdateEvent) {
      doMetadataUpdate((Event.MetadataUpdateEvent) event);
    } else if (event instanceof Event.RenameEvent) {
      doRename((Event.RenameEvent) event);
    } else if (event instanceof Event.UnlinkEvent) {
      doUnlink((Event.UnlinkEvent) event);
    } else {
      throw new UnsupportedOperationException("Unrecognized event " + event);
    }
  }

  protected void doAppend(Event.AppendEvent event) {
    LOG.info("APPEND(path={})", event.getPath());
  }

  protected void doClose(Event.CloseEvent event) {
    LOG.info("CLOSE(filesize={}, path={}, ts={})",
      event.getFileSize(), event.getPath(), event.getTimestamp());
  }

  protected void doCreate(Event.CreateEvent event) {
    LOG.info("CREATE(group={}, owner={}, path={})",
      event.getGroupName(), event.getOwnerName(), event.getPath());
  }

  protected void doMetadataUpdate(Event.MetadataUpdateEvent event) {
    LOG.info("METADATA(acls={}, group={}, owner={}, path={}, atime={}, perms={})",
      event.getAcls(), event.getGroupName(), event.getOwnerName(), event.getPath(),
      event.getAtime(), event.getPerms());
  }

  protected void doRename(Event.RenameEvent event) {
    LOG.info("RENAME(ts={}, src={}, dst={})",
      event.getTimestamp(), event.getSrcPath(), event.getDstPath());
  }

  protected void doUnlink(Event.UnlinkEvent event) {
    LOG.info("UNLINK(ts={}, path={})", event.getTimestamp(), event.getPath());
  }
}
