package com.github.lightcopy.fs;

import java.util.UUID;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.bson.Document;

/**
 * Abstract class that represents node of the file system.
 */
public abstract class INode {
  /**
   * [[INodeType]] class represents type of the nodes in file system. For example, generally types
   * are: directory, file, and symlink.
   */
  public static abstract class INodeType {
    /** Unique name of the type */
    public abstract String getName();

    /** Whether or not such node is leaf node, and does not allow children nodes */
    public abstract boolean isLeafNode();

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof INodeType)) return false;
      INodeType tpe = (INodeType) other;
      return tpe.getName().equals(getName());
    }

    @Override
    public String toString() {
      return getName();
    }
  }

  /** Type for directory inode */
  public static class DirectoryType extends INodeType {
    @Override
    public String getName() {
      return "DIRECTORY";
    }

    @Override
    public boolean isLeafNode() {
      return true;
    }
  }

  /** Type for file inode */
  public static class FileType extends INodeType {
    @Override
    public String getName() {
      return "FILE";
    }

    @Override
    public boolean isLeafNode() {
      return false;
    }
  }

  /** Type for symlink inode */
  public static class SymlinkType extends INodeType {
    @Override
    public String getName() {
      return "SYMLINK";
    }

    @Override
    public boolean isLeafNode() {
      return false;
    }
  }

  public static final String UUID_COLUMN = "uuid";

  /** Generate random id for inode (for insertion only) */
  private static String nextUUID() {
    return UUID.randomUUID().toString();
  }

  // globally unique id
  private String uuid;
  // access time and modification time
  private long accessTime;
  private long modificationTime;
  // file system statistics and size
  private long sizeBytes;
  private long diskUsageBytes;
  private long blockSizeBytes;
  private short replicationFactor;
  // inode access
  private INodeAccess access;
  // inode name
  private String name;
  // inode parent, can be null if path is root directory
  private INode parent;
  // inode type
  private INodeType tpe;

  public INode(FileStatus status, INodeType tpe) {
    this.uuid = nextUUID();
    this.accessTime = status.getAccessTime();
    this.modificationTime = status.getModificationTime();
    this.sizeBytes = status.getLen();
    // disk usage defaults to size in bytes, for directories this
    // has to be set to leaf total size
    this.diskUsageBytes = this.sizeBytes;
    this.blockSizeBytes = status.getBlockSize();
    this.replicationFactor = status.getReplication();
    // parse permissions and access
    this.access = new INodeAccess(status.getOwner(), status.getGroup(), status.getPermission());
    // parse path
    this.name = status.getPath().getName();
    this.tpe = tpe;
  }

  public void setParent(INode parent) {
    this.parent = parent;
  }

  public void setDiskUsage(long bytes) {
    if (bytes < 0) throw new IllegalArgumentException("Negative bytes " + bytes + "in disk usage");
    this.diskUsageBytes = bytes;
  }

  public void addDiskUsage(long bytes) {
    if (bytes < 0) throw new IllegalArgumentException("Negative bytes " + bytes + "in disk usage");
    setDiskUsage(this.diskUsageBytes + bytes);
  }

  public String getId() {
    return this.uuid;
  }

  public long getAccessTime() {
    return this.accessTime;
  }

  public long getModificationTime() {
    return this.modificationTime;
  }

  public long getSize() {
    return this.sizeBytes;
  }

  public long getDiskUsage() {
    return this.diskUsageBytes;
  }

  public long getBlockSize() {
    return this.blockSizeBytes;
  }

  public short getReplicationFactor() {
    return this.replicationFactor;
  }

  public INodeAccess getAccessInfo() {
    return this.access;
  }

  public String getName() {
    return this.name;
  }

  public INode getParent() {
    return this.parent;
  }

  public INodeType getType() {
    return this.tpe;
  }

  /** Convert inode into Mongo document specification by using columns defined above */
  public Document toDocument() {
    return null;
  }
}
