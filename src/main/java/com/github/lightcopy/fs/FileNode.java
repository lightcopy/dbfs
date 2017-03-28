package com.github.lightcopy.fs;

import org.apache.hadoop.fs.FileStatus;

public class FileNode extends INode {
  public FileNode(FileStatus status) {
    super(status, new INode.FileType());
  }
}
