package com.github.lightcopy.fs;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;

/**
 * Internal implementation of the tree visitor for HDFS manager.
 */
public class NodeTreeVisitor implements TreeVisitor {
  // mongo file system to store nodes
  private final MongoFileSystem fs;
  // leaf nodes that can be inserted directly
  private ArrayList<INode> leaves;
  // current inode
  private INode current;

  public NodeTreeVisitor(MongoFileSystem fs) {
    this.fs = fs;
    this.current = null;
    this.leaves = new ArrayList<INode>();
  }

  protected INode getCurrent() {
    return this.current;
  }

  @Override
  public void visitBefore(FileStatus root) {
    this.current = new INode(root);
  }

  @Override
  public void visitChild(FileStatus child) {
    this.leaves.add(new INode(child));
  }

  @Override
  public void visitChild(TreeVisitor visitor) {
    // right now we do need to check level visitor
  }

  @Override
  public void visitAfter() {
    try {
      // insert leaves + current node
      this.leaves.add(this.current);
      this.fs.insert(this.leaves);
      // clear all children and leaves
      this.leaves = null;
    } catch (IOException ioe) {
      throw new RuntimeException("Failed to traverse nodes, reason: " + ioe, ioe);
    }
  }
}
