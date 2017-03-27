package com.github.lightcopy.fs;

import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Represents all access properties for the inode, including permissions, owner and group.
 */
public class INodeAccess {
  private String group;
  private String owner;
  // permission should be consistent with permissionMask
  private String permission;
  private short permissionMask;

  public INodeAccess(String owner, String group, FsPermission perm) {
    if (owner == null) throw new IllegalArgumentException("Owner field is null");
    if (group == null) throw new IllegalArgumentException("Group field is null");
    this.owner = owner;
    this.group = group;
    this.permission = perm.toString();
    this.permissionMask = perm.toShort();
  }

  @Override
  public String toString() {
    return "[owner=" + this.owner +
      ", group=" + this.group +
      ", permission=" + this.permission + "(" + this.permissionMask + ")]";
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

  public short getPermissionMask() {
    return this.permissionMask;
  }
}
