package com.github.lightcopy.fs;

import java.util.ArrayList;

import org.bson.conversions.Bson;
import com.mongodb.client.model.Updates;

/**
 * Holder for updatable properties also known as file metadata. Does not support all properties
 * provided by hadoop file status. If INode class changes, those updates should be reflected here.
 */
public class INodeUpdate {
  // inode access time, must be > 0 to update
  private long accessTime;
  // inode modification time, must be > 0 to update
  private long modificationTime;
  // inode replication factor, must be > 0 to update
  private int replicationFactor;
  // inode group, must be not null to update
  private String group;
  // inode owner, must be not null to update
  private String owner;
  // inode permission, must be not null to update
  private String permission;
  // inode file size in bytes, must be > 0 to update
  private long sizeBytes;

  public INodeUpdate() { }

  public INodeUpdate setAtime(long time) {
    this.accessTime = time;
    return this;
  }

  public INodeUpdate setMtime(long time) {
    this.modificationTime = time;
    return this;
  }

  public INodeUpdate setGroup(String group) {
    this.group = group;
    return this;
  }

  public INodeUpdate setOwner(String owner) {
    this.owner = owner;
    return this;
  }

  public INodeUpdate setPermission(String permission) {
    this.permission = permission;
    return this;
  }

  public INodeUpdate setReplication(int replication) {
    this.replicationFactor = replication;
    return this;
  }

  /** This is added for close event, metadata event does not update this field */
  public INodeUpdate setFileSize(long bytes) {
    this.sizeBytes = bytes;
    return this;
  }

  /** Return Bson object with updates */
  public Bson bson() {
    ArrayList<Bson> batch = new ArrayList<Bson>();
    if (this.accessTime > 0) {
      batch.add(Updates.set(INode.FIELD_ACCESS_TIME, this.accessTime));
    }

    if (this.modificationTime > 0) {
      batch.add(Updates.set(INode.FIELD_MODIFICATION_TIME, this.modificationTime));
    }

    if (this.replicationFactor > 0) {
      batch.add(Updates.set(INode.FIELD_REPLICATION_FACTOR, this.replicationFactor));
    }

    if (this.group != null) {
      batch.add(Updates.set(INode.FIELD_GROUP, this.group));
    }

    if (this.owner != null) {
      batch.add(Updates.set(INode.FIELD_OWNER, this.owner));
    }

    if (this.permission != null) {
      batch.add(Updates.set(INode.FIELD_PERMISSION, this.permission));
    }

    if (this.sizeBytes > 0) {
      batch.add(Updates.set(INode.FIELD_SIZE_BYTES, this.sizeBytes));
    }
    // if batch does not contain any updates return null, indicating that update should be ignored
    // upstream
    if (batch.size() == 0) return null;
    return Updates.combine(batch);
  }
}
