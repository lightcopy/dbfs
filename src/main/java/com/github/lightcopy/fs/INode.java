package com.github.lightcopy.fs;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

public abstract class INode {
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
  // inode path + parent, can be null if path is root directory
  private String parentPath;
  private String path;

  public INode(FileStatus status) {
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
    Path statusPath = status.getPath();
    this.path = statusPath.toString();
    if (statusPath.getParent() != null) {
      this.parentPath = statusPath.getParent().toString();
    }
  }
}
