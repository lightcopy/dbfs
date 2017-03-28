package com.github.lightcopy.fs;

import org.apache.hadoop.fs.permission.FsPermission;

import org.bson.Document;

import com.github.lightcopy.mongo.DocumentLike;

/**
 * Represents all access properties for the inode, including permissions, owner and group.
 */
public class INodeAccess implements DocumentLike<INodeAccess> {
  // Columns for document, must be unique
  public static final String FIELD_OWNER = "owner";
  public static final String FIELD_GROUP = "group";
  public static final String FIELD_PERMISSION = "permission";

  private String owner;
  private String group;
  private String permission;

  public INodeAccess(String owner, String group, FsPermission perm) {
    if (owner == null) throw new IllegalArgumentException("Owner field is null");
    if (group == null) throw new IllegalArgumentException("Group field is null");
    this.owner = owner;
    this.group = group;
    this.permission = perm.toString();
  }

  /** Constructor to create from Document */
  public INodeAccess() { }

  @Override
  public String toString() {
    return "[owner=" + this.owner +
      ", group=" + this.group +
      ", permission=" + this.permission + "]";
  }

  public String getOwner() {
    return this.owner;
  }

  public String getGroup() {
    return this.group;
  }

  public String getPermission() {
    return this.permission;
  }

  @Override
  public Document toDocument() {
    return new Document()
      .append(FIELD_OWNER, getOwner())
      .append(FIELD_GROUP, getGroup())
      .append(FIELD_PERMISSION, getPermission());
  }

  @Override
  public INodeAccess fromDocument(Document doc) {
    this.owner = doc.getString(FIELD_OWNER);
    this.group = doc.getString(FIELD_GROUP);
    this.permission = doc.getString(FIELD_PERMISSION);
    return this;
  }
}
