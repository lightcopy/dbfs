package com.github.lightcopy.codec;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.bson.Document;

/**
 * [[INode]] represents node of the file system.
 */
public class INode {
  /** All available types of inode: directory, file, or symbolic link */
  static enum INodeType {
    DIRECTORY, FILE, SYMLINK
  }

  // Columns of Document instance, must all be unique
  public static final String FIELD_UUID = "uuid";
  public static final String FIELD_ACCESS_TIME = "accessTime";
  public static final String FIELD_MODIFICATION_TIME  = "modificationTime";
  public static final String FIELD_SIZE_BYTES = "sizeBytes";
  public static final String FIELD_BLOCK_SIZE_BYTES = "blockSizeBytes";
  public static final String FIELD_REPLICATION_FACTOR = "replicationFactor";
  public static final String FIELD_GROUP = "group";
  public static final String FIELD_OWNER = "owner";
  public static final String FIELD_PERMISSION = "permission";
  public static final String FIELD_NAME = "name";
  public static final String FIELD_PARENT = "parent";
  public static final String FIELD_TYPE = "type";
  public static final String FIELD_PATH = "path";

  // access time and modification time
  private long accessTime;
  private long modificationTime;
  // file system statistics and size
  private long sizeBytes;
  private long diskUsageBytes;
  private long blockSizeBytes;
  private int replicationFactor;
  // inode access control
  private String group;
  private String owner;
  private String permission;

  // inode name
  private String name;
  // inode type
  private INodeType nodeType;
  // inode fs path
  private INodePath path;

  public INode(FileStatus status) {
    this(status.getAccessTime(), status.getModificationTime(), status.getLen(),
      status.getBlockSize(), status.getReplication(), status.getGroup(), status.getOwner(),
      status.getPermission().toString(), status.getPath().getName(),
      new INodePath(status.getPath()), statusType(status));
  }

  public INode(long accessTime, long modificationTime, long size, long blockSize,
      int replicationFactor, String group, String owner, String permission, String name,
      INodePath path, String nodeType) {
    this(accessTime, modificationTime, size, blockSize, replicationFactor, group, owner,
      permission, name, path, INodeType.valueOf(nodeType));
  }

  protected INode(long accessTime, long modificationTime, long size, long blockSize,
      int replicationFactor, String group, String owner, String permission, String name,
      INodePath path, INodeType nodeType) {
    this.accessTime = accessTime;
    this.modificationTime = modificationTime;
    this.sizeBytes = size;
    this.blockSizeBytes = blockSize;
    this.replicationFactor = replicationFactor;
    // parse permissions and access
    this.group = group;
    this.owner = owner;
    this.permission = permission;
    // parse path
    this.name = name;
    this.path = path;
    // select appropriate type for inode
    this.nodeType = nodeType;
  }

  /** Empty constructor for builder pattern */
  protected INode() { }

  /** Convert file status path into inode path */
  private static INodePath statusPath(FileStatus status) {
    return new INodePath(status.getPath());
  }

  /** Convert file status into known inode type */
  private static INodeType statusType(FileStatus status) {
    if (status.isDirectory()) {
      return INodeType.DIRECTORY;
    } else if (status.isFile()) {
      return INodeType.FILE;
    } else {
      return INodeType.SYMLINK;
    }
  }

  protected INode setAccessTime(long value) {
    this.accessTime = value;
    return this;
  }

  protected INode setModificationTime(long value) {
    this.modificationTime = value;
    return this;
  }

  protected INode setSize(long bytes) {
    this.sizeBytes = bytes;
    return this;
  }

  protected INode setBlockSize(long bytes) {
    this.blockSizeBytes = bytes;
    return this;
  }

  protected INode setReplicationFactor(int value) {
    this.replicationFactor = value;
    return this;
  }

  protected INode setGroup(String value) {
    this.group = value;
    return this;
  }

  protected INode setOwner(String value) {
    this.owner = value;
    return this;
  }

  protected INode setPermission(String value) {
    this.permission = value;
    return this;
  }

  protected INode setName(String value) {
    this.name = value;
    return this;
  }

  protected INode setTypeName(String value) {
    this.nodeType = INodeType.valueOf(value);
    return this;
  }

  protected INode setPath(INodePath path) {
    this.path = path;
    return this;
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


  public long getBlockSize() {
    return this.blockSizeBytes;
  }


  public int getReplicationFactor() {
    return this.replicationFactor;
  }


  public String getGroup() {
    return this.group;
  }


  public String getOwner() {
    return this.owner;
  }


  public String getPermission() {
    return this.permission;
  }


  public String getName() {
    return this.name;
  }

  public String getTypeName() {
    return this.nodeType.name();
  }

  public INodePath getPath() {
    return this.path;
  }

  @Override
  public String toString() {
    return this.getTypeName() +
      "(name=" + this.name +
      ", path=" + this.path +
      ", accessTime=" + this.accessTime +
      ", modificationTime=" + this.modificationTime +
      ", size=" + this.sizeBytes +
      ", blockSize=" + this.blockSizeBytes +
      ", replicationFactor=" + this.replicationFactor +
      ", group=" + this.group +
      ", owner=" + this.owner +
      ", permission=" + this.permission + ")";
  }
}
