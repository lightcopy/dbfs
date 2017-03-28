package com.github.lightcopy.fs;

import org.apache.hadoop.fs.FileStatus;

public class DirectoryNode extends INode {
  public DirectoryNode(FileStatus status) {
    super(status, new INode.DirectoryType());
  }
}
