package com.github.lightcopy.codec;

import java.io.IOException;
import java.util.concurrent.locks.ReentrantLock;

import org.bson.BsonDocument;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;

import com.mongodb.Block;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper on MongoCollection for INode.
 * Provides some basic methods to traverse collection in file system manner.
 */
public class MongoFileSystem {
  private static final Logger LOG = LoggerFactory.getLogger(MongoFileSystem.class);

  // lock for modification operations
  private final ReentrantLock modificationLock;
  // underlying collection that serves as file system
  private final MongoCollection<INode> fs;

  public MongoFileSystem(MongoCollection<?> collection) {
    this.modificationLock = new ReentrantLock();
    CodecRegistry registry = CodecRegistries.fromCodecs(new INodeCodec());
    this.fs = collection.withCodecRegistry(registry).withDocumentClass(INode.class);
  }

  /** Helper method to compute duration in milliseconds */
  private double millis(long start, long end) {
    return (end - start) / 1e6;
  }

  /** Get inode from file system, returns null if none found */
  private INode doGet(INodePath path) {
    return this.fs.find(FsFilters.path(path)).first();
  }

  /** Delete inode from file system; deletion is always recursive */
  private void doDelete(INodePath path) throws IOException {
    DeleteResult result = this.fs.deleteMany(FsFilters.paths(path));
    if (!result.wasAcknowledged()) {
      throw new IOException("Failed to delete path " + path + ", result was not acknowledged");
    }
    LOG.info("Deleted {} nodes for path {}", result.getDeletedCount(), path);
  }

  /** Insert new node into file system, if another node exists for the path, will replace it */
  private void doUpsert(INode node) throws IOException {
    INodePath path = node.getPath();
    UpdateOptions options = new UpdateOptions().upsert(true);
    UpdateResult result = this.fs.replaceOne(FsFilters.path(path), node, options);
    if (!result.wasAcknowledged()) {
      throw new IOException("Failed to insert path " + path + ", result was not acknowledged");
    }
    LOG.info("Inserted node {} with id {}, modified count {}", node, result.getUpsertedId(),
      result.getModifiedCount());
  }

  /** Rename src path into dst path */
  private void doRename(final INodePath srcPath, final INodePath dstPath) throws IOException {
    Block<INode> renameBlock = new Block<INode>() {
      @Override
      public void apply(INode node) {
        INodePath path = node.getPath();
        // modify existing node to have updated path and replace in collection
        node.setPath(path.withUpdatedPrefix(srcPath, dstPath));
        UpdateResult result = fs.replaceOne(FsFilters.path(path), node);
        if (!result.wasAcknowledged()) {
          throw new RuntimeException("Failed to update node " + node +
            ", result was not acknowledged");
        }
        LOG.info("Updated node {}, modified count {} = 1", node, result.getModifiedCount());
      }
    };
    this.fs.find(FsFilters.paths(srcPath)).forEach(renameBlock);
    LOG.info("Updated nodes from {} to {}", srcPath, dstPath);
  }

  /** Update node for path using provided batch of updates */
  private void doUpdate(INodePath path, INodeUpdate builder) throws IOException {
    Bson update = builder.bson();
    if (update != null) {
      UpdateResult result = this.fs.updateOne(FsFilters.path(path), update);
      if (!result.wasAcknowledged()) {
        throw new IOException("Failed to update path " + path + " with update " + update);
      }
      LOG.info("Modified path {} with update {}, modified count {} = 1", path, update,
        result.getModifiedCount());
    } else {
      LOG.warn("Update was ignored, because bson update is null for path {}", path);
    }
  }

  //////////////////////////////////////////////////////////////
  // Public operators
  //////////////////////////////////////////////////////////////

  /**
   * Get node for path, method is readonly.
   * @param path path to retrieve
   * @return INode instance for that path
   */
  public INode get(INodePath path) {
    long startTime = System.nanoTime();
    try {
      return doGet(path);
    } finally {
      long endTime = System.nanoTime();
      LOG.info("Get operation took {} ms", millis(startTime, endTime));
    }
  }

  /**
   * Delete path recursively with lock. Note that this operation is not atomic.
   * @param path path to delete
   */
  public void delete(INodePath path) throws IOException {
    this.modificationLock.lock();
    long startTime = System.nanoTime();
    try {
      doDelete(path);
    } finally {
      this.modificationLock.unlock();
      long endTime = System.nanoTime();
      LOG.info("Delete operation took {} ms", millis(startTime, endTime));
    }
  }

  /**
   * Insert node if one does not exist for the path, otherwise replace existing node with provided.
   * Operation is atomic.
   * @param node node to insert or replace with
   */
  public void upsert(INode node) throws IOException {
    this.modificationLock.lock();
    long startTime = System.nanoTime();
    try {
      doUpsert(node);
    } finally {
      this.modificationLock.unlock();
      long endTime = System.nanoTime();
      LOG.info("Upsert operation took {} ms", millis(startTime, endTime));
    }
  }

  /**
   * Rename path from srcPath to dstPath recursively. Operation is not atomic.
   * @param srcPath path to replace
   * @param dstPath path to use as a replacement
   */
  public void rename(INodePath srcPath, INodePath dstPath) throws IOException {
    this.modificationLock.lock();
    long startTime = System.nanoTime();
    try {
      doRename(srcPath, dstPath);
    } finally {
      this.modificationLock.unlock();
      long endTime = System.nanoTime();
      LOG.info("Rename operation took {} ms", millis(startTime, endTime));
    }
  }

  /**
   * Update individual node for provided path with update batch. Note that if update batch does not
   * contain any modification, operation is ignored. Operation is atomic.
   * @param path path for node to update
   * @param builder node update builder
   */
  public void update(INodePath path, INodeUpdate builder) throws IOException {
    this.modificationLock.lock();
    long startTime = System.nanoTime();
    try {
      doUpdate(path, builder);
    } finally {
      this.modificationLock.unlock();
      long endTime = System.nanoTime();
      LOG.info("Update operation took {} ms", millis(startTime, endTime));
    }
  }
}
