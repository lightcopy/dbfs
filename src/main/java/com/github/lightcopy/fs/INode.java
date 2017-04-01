package com.github.lightcopy.fs;

import java.util.UUID;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.bson.Document;

import com.github.lightcopy.mongo.DocumentLike;

/**
 * [[INode]] represents node of the file system.
 */
public class INode implements DocumentLike<INode> {
  // Columns of Document instance, must all be unique
  public static final String FIELD_UUID = "uuid";
  public static final String FIELD_ACCESS_TIME = "accessTime";
  public static final String FIELD_MODIFICATION_TIME  = "modificationTime";
  public static final String FIELD_SIZE_BYTES = "sizeBytes";
  public static final String FIELD_DISK_USAGE_BYTES = "diskUsageBytes";
  public static final String FIELD_BLOCK_SIZE_BYTES = "blockSizeBytes";
  public static final String FIELD_REPLICATION_FACTOR = "replicationFactor";
  public static final String FIELD_ACCESS = "access";
  public static final String FIELD_NAME = "name";
  public static final String FIELD_PARENT = "parent";
  public static final String FIELD_TYPE = "type";
  public static final String FIELD_PATH = "path";

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
  private int replicationFactor;
  // inode access
  private INodeAccess access;
  // inode name
  private String name;
  // inode type
  private INodeType tpe;
  // inode fs path
  private INodePath path;

  public INode(FileStatus status) {
    try {
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
      // select appropriate type for inode
      if (status.isDirectory()) {
        this.tpe = INodeType.DIRECTORY;
      } else if (status.isFile()) {
        this.tpe = INodeType.FILE;
      } else if (status.isSymlink()) {
        this.tpe = INodeType.SYMLINK;
      } else {
        throw new UnsupportedOperationException("Unknown type for " + status);
      }

      this.path = new INodePath(status.getPath());
    } catch (Exception err) {
      throw new RuntimeException("Failed to create inode from status " + status, err);
    }
  }

  /** Constructor to create from Document */
  public INode() { }

  public void setDiskUsage(long bytes) {
    if (bytes < 0) throw new IllegalArgumentException("Negative bytes " + bytes + "in disk usage");
    this.diskUsageBytes = bytes;
  }

  public void addDiskUsage(long bytes) {
    setDiskUsage(this.diskUsageBytes + bytes);
  }

  public void setFileSize(long bytes) {
    if (bytes < 0) throw new IllegalArgumentException("Negative bytes " + bytes + "in file size");
    this.sizeBytes = bytes;
  }

  /** Replace prefix srcPath with dstPath */
  public INode replacePrefix(INodePath srcPath, INodePath dstPath) {
    // if node does not have srcPath as prefix, throw exception as we cannot update such path
    int srcLen = srcPath.getDepth();
    int dstLen = dstPath.getDepth();
    int len = this.path.getDepth();
    for (int i = 0; i < srcLen; i++) {
      if (i >= len || !this.path.getElem(i).equals(srcPath.getElem(i))) {
        throw new IllegalArgumentException(this.path + " does not have expected prefix " + srcPath);
      }
    }

    Path raw = dstPath.getPath();
    while (srcLen < len) {
      raw = new Path(raw, this.path.getElem(srcLen));
      srcLen++;
    }
    this.path = new INodePath(raw);
    return this;
  }

  public void setAccessTime(long time) {
    this.accessTime = time;
  }

  public void setModificationTime(long time) {
    this.modificationTime = time;
  }

  public void setReplicationFactor(int value) {
    this.replicationFactor = value;
  }

  public String getUUID() {
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

  public int getReplicationFactor() {
    return this.replicationFactor;
  }

  public INodeAccess getAccessInfo() {
    return this.access;
  }

  public String getName() {
    return this.name;
  }

  public INodeType getType() {
    return this.tpe;
  }

  /** Return type as string */
  public String getTypeName() {
    return this.tpe.name();
  }

  public INodePath getPath() {
    return this.path;
  }

  @Override
  public Document toDocument() {
    return new Document()
      .append(FIELD_UUID, getUUID())
      .append(FIELD_ACCESS_TIME, getAccessTime())
      .append(FIELD_MODIFICATION_TIME, getModificationTime())
      .append(FIELD_SIZE_BYTES, getSize())
      .append(FIELD_DISK_USAGE_BYTES, getDiskUsage())
      .append(FIELD_BLOCK_SIZE_BYTES, getBlockSize())
      .append(FIELD_REPLICATION_FACTOR, getReplicationFactor())
      .append(FIELD_ACCESS, getAccessInfo().toDocument())
      .append(FIELD_NAME, getName())
      .append(FIELD_TYPE, getTypeName())
      .append(FIELD_PATH, getPath().toDocument());
  }

  @Override
  public INode fromDocument(Document doc) {
    this.uuid = doc.getString(FIELD_UUID);
    this.accessTime = doc.getLong(FIELD_ACCESS_TIME);
    this.modificationTime = doc.getLong(FIELD_MODIFICATION_TIME);
    this.sizeBytes = doc.getLong(FIELD_SIZE_BYTES);
    this.diskUsageBytes = doc.getLong(FIELD_DISK_USAGE_BYTES);
    this.blockSizeBytes = doc.getLong(FIELD_BLOCK_SIZE_BYTES);
    this.replicationFactor = doc.getInteger(FIELD_REPLICATION_FACTOR);
    this.access = new INodeAccess().fromDocument(doc.get(FIELD_ACCESS, Document.class));
    this.name = doc.getString(FIELD_NAME);
    this.tpe = INodeType.valueOf(doc.getString(FIELD_TYPE));
    this.path = new INodePath().fromDocument(doc.get(FIELD_PATH, Document.class));
    return this;
  }
}
