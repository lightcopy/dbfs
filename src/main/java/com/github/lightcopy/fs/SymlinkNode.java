package com.github.lightcopy.fs;

import org.apache.hadoop.fs.FileStatus;

public class SymlinkNode extends INode {
  public SymlinkNode(FileStatus status) {
    super(status, new INode.SymlinkType());
  }
}
