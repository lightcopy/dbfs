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

import com.github.lightcopy.mongo.MongoUtils;

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

    if (event instanceof Event.AppendEvent) {
      doAppend((Event.AppendEvent) event, transactionId);
    } else if (event instanceof Event.CloseEvent) {
      doClose((Event.CloseEvent) event, transactionId);
    } else if (event instanceof Event.CreateEvent) {
      doCreate((Event.CreateEvent) event, transactionId);
    } else if (event instanceof Event.MetadataUpdateEvent) {
      doMetadataUpdate((Event.MetadataUpdateEvent) event, transactionId);
    } else if (event instanceof Event.RenameEvent) {
      doRename((Event.RenameEvent) event, transactionId);
    } else if (event instanceof Event.UnlinkEvent) {
      doUnlink((Event.UnlinkEvent) event, transactionId);
    } else {
      throw new UnsupportedOperationException("Unrecognized event " + event);
    }
  }

  /**
   * Updae all parents for provided path with new information. For now, it is just total disk usage
   * for inode.
   */
  private void updateParentInfo(INodePath path, long diskUsageBytes) {
    Iterator<INode> iter = MongoUtils.findParents(this.manager.mongoFileSystem(), path);
    while (iter.hasNext()) {
      INode parent = iter.next();
      LOG.info("Updated path {} with disk usage {}", parent.getPath().getPath(), diskUsageBytes);
      parent.addDiskUsage(diskUsageBytes);
      long updated = MongoUtils.update(this.manager.mongoFileSystem(), parent);
      LOG.info("Updated {} documents", updated);
    }
  }

  protected void doAppend(Event.AppendEvent event, long transactionId) throws IOException {
    // we do not process append events
    LOG.info("APPEND(path={})", event.getPath());
  }

  protected void doClose(Event.CloseEvent event, long transactionId) {
    LOG.info("CLOSE(filesize={}, path={}, ts={})",
      event.getFileSize(), event.getPath(), event.getTimestamp());
    INodePath path = new INodePath(new Path(event.getPath()));
    INode node = MongoUtils.find(this.manager.mongoFileSystem(), path);
    long delta = event.getFileSize() - node.getDiskUsage();
    node.setFileSize(event.getFileSize());
    node.addDiskUsage(delta);
    MongoUtils.update(this.manager.mongoFileSystem(), node);
    updateParentInfo(path, delta);
  }

  protected void doCreate(Event.CreateEvent event, long transactionId) throws IOException {
    LOG.info("CREATE(group={}, owner={}, path={})",
      event.getGroupName(), event.getOwnerName(), event.getPath());
    FileStatus status = this.manager.getFileSystem().getFileStatus(new Path(event.getPath()));
    INode node = new INode(status);
    MongoUtils.insert(this.manager.mongoFileSystem(), node);
    updateParentInfo(node.getPath(), node.getDiskUsage());
  }

  protected void doMetadataUpdate(Event.MetadataUpdateEvent event, long transactionId) {
    LOG.info("METADATA(acls={}, group={}, owner={}, path={}, atime={}, perms={})",
      event.getAcls(), event.getGroupName(), event.getOwnerName(), event.getPath(),
      event.getAtime(), event.getPerms());
    INodePath path = new INodePath(new Path(event.getPath()));
    INode node = MongoUtils.find(this.manager.mongoFileSystem(), path);
    if (event.getAtime() > 0) {
      node.setAccessTime(event.getAtime());
    }

    if (event.getMtime() > 0) {
      node.setModificationTime(event.getMtime());
    }

    if (event.getReplication() > 0) {
      node.setReplicationFactor(event.getReplication());
    }

    if (event.getGroupName() != null) {
      node.getAccessInfo().setGroup(event.getGroupName());
    }

    if (event.getOwnerName() != null) {
      node.getAccessInfo().setOwner(event.getOwnerName());
    }

    if (event.getPerms() != null) {
      node.getAccessInfo().setPermission(event.getPerms());
    }

    long updated = MongoUtils.update(this.manager.mongoFileSystem(), node);
    LOG.info("Updated {} documents", updated);
  }

  protected void doRename(Event.RenameEvent event, long transactionId) {
    LOG.info("RENAME(ts={}, src={}, dst={})",
      event.getTimestamp(), event.getSrcPath(), event.getDstPath());
    // find all paths with prefix of src path, replace src path prefix with dst path, update inodes
    // and replace them in file system
    INodePath srcPath = new INodePath(new Path(event.getSrcPath()));
    INodePath dstPath = new INodePath(new Path(event.getDstPath()));
    Iterator<INode> iter = MongoUtils.findAll(this.manager.mongoFileSystem(), srcPath);
    long diskUsageBytes = 0;
    while (iter.hasNext()) {
      INode node = iter.next();
      // update disk usage for the srcPath inode
      if (node.getPath().getPath() == srcPath.getPath()) {
        diskUsageBytes = node.getDiskUsage();
      }
      // update path prefix for the node in place
      node.replacePrefix(srcPath, dstPath);
      long updated = MongoUtils.update(this.manager.mongoFileSystem(), node);
      LOG.info("Updated {} documents", updated);
    }
    // update source parents with new disk usage
    updateParentInfo(srcPath, -diskUsageBytes);
    // update destination parents with new disk usage
    updateParentInfo(dstPath, diskUsageBytes);
  }

  /**
   * Process event that deletes object from file system. In this particular case we do not need to
   * fetch file status from hdfs, just reconstruct path and delete recursively all child paths.
   */
  protected void doUnlink(Event.UnlinkEvent event, long transactionId) {
    LOG.info("UNLINK(ts={}, path={})", event.getTimestamp(), event.getPath());
    // fetch path from collection, reconstruct inode, get file size, delete inode, update file size
    INodePath path = new INodePath(new Path(event.getPath()));
    INode node = MongoUtils.find(this.manager.mongoFileSystem(), path);
    if (node == null) {
      LOG.warn("Did not find node for path {}", event.getPath());
    } else {
      // update state of file system
      long diskUsageBytes = node.getDiskUsage();
      long deletedCount = MongoUtils.deleteRecursively(this.manager.mongoFileSystem(), path);
      LOG.info("Deleted {} nodes for path {}", deletedCount, event.getPath());
      // negative disk usage since we remove path
      updateParentInfo(path, -diskUsageBytes);
    }
  }
}
