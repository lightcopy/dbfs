package com.github.lightcopy.fs;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
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
        LOG.error("Thread interrupted", err);
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
    // always save event before moving to file system
    this.manager.mongoEventPool().insert(new EventContainer(transactionId, event));
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

  protected void doAppend(Event.AppendEvent event, long transactionId) {
    // append is triggered when file is opened for append, this does not really update file system,
    // we instead listen to close events.
    LOG.info("APPEND(path={})", event.getPath());
  }

  protected void doClose(Event.CloseEvent event, long transactionId) throws IOException {
    LOG.info("CLOSE(filesize={}, path={}, ts={})",
      event.getFileSize(), event.getPath(), event.getTimestamp());
    INodePath path = new INodePath(event.getPath());
    INodeUpdate update = new INodeUpdate()
      .setFileSize(event.getFileSize())
      .setMtime(event.getTimestamp());
    this.manager.mongoFileSystem().update(path, update);
  }

  protected void doCreate(Event.CreateEvent event, long transactionId) throws IOException {
    LOG.info("CREATE(group={}, owner={}, path={})",
      event.getGroupName(), event.getOwnerName(), event.getPath());
    // reconstruct file status from create event. For directory, it is okay, because we would not
    // need to update file size. For files, close event will be called after creation, which would
    // update file size on this path.
    // when copying files from local to hdfs, chain of events is triggered create -> close -> rename
    // in this case we execute them in exact same order, relying on lock for mongo fs.
    if (event.getOverwrite()) {
      // delete previous file
      INodePath path = new INodePath(event.getPath());
      this.manager.mongoFileSystem().delete(path);
      LOG.info("Delete previous file {}, event overwrites file", event.getPath());
    }

    long defaultSize = 0L;
    long defaultBlockSize = event.getDefaultBlockSize();
    boolean isDirectory = event.getiNodeType() == Event.CreateEvent.INodeType.DIRECTORY;
    int replication = event.getReplication();
    long accessTime = event.getCtime();
    long modificationTime = event.getCtime();
    FsPermission permission = new FsPermission(event.getPerms());
    Path symlinkPath = (event.getSymlinkTarget() == null) ? null : new Path(event.getSymlinkTarget());
    Path path = new Path(event.getPath());

    FileStatus status = new FileStatus(defaultSize, isDirectory, replication, defaultBlockSize,
      modificationTime, accessTime, permission, event.getOwnerName(), event.getGroupName(),
      symlinkPath, path);
    INode node = new INode(status);
    this.manager.mongoFileSystem().upsert(node);
  }

  protected void doMetadataUpdate(
      Event.MetadataUpdateEvent event,
      long transactionId) throws IOException {
    LOG.info("METADATA(acls={}, group={}, owner={}, path={}, atime={}, perms={})",
      event.getAcls(), event.getGroupName(), event.getOwnerName(), event.getPath(),
      event.getAtime(), event.getPerms());
    INodePath path = new INodePath(event.getPath());
    INodeUpdate update = new INodeUpdate()
      .setAtime(event.getAtime())
      .setGroup(event.getGroupName())
      .setMtime(event.getMtime())
      .setOwner(event.getOwnerName())
      .setPermission(event.getPerms())
      .setReplication(event.getReplication());
    this.manager.mongoFileSystem().update(path, update);
  }

  protected void doRename(Event.RenameEvent event, long transactionId) throws IOException {
    LOG.info("RENAME(ts={}, src={}, dst={})",
      event.getTimestamp(), event.getSrcPath(), event.getDstPath());
    INodePath srcPath = new INodePath(event.getSrcPath());
    INodePath dstPath = new INodePath(event.getDstPath());
    this.manager.mongoFileSystem().rename(srcPath, dstPath);
  }

  protected void doUnlink(Event.UnlinkEvent event, long transactionId) throws IOException {
    LOG.info("UNLINK(ts={}, path={})", event.getTimestamp(), event.getPath());
    INodePath path = new INodePath(event.getPath());
    this.manager.mongoFileSystem().delete(path);
  }
}
