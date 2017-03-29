package com.github.lightcopy.fs;

import java.util.ArrayList;

import org.apache.hadoop.fs.FileStatus;

import org.bson.Document;
import com.mongodb.client.model.Filters;
import com.mongodb.client.MongoCollection;

/**
 * Internal implementation of the tree visitor for HDFS manager.
 */
public class NodeTreeVisitor implements TreeVisitor {
  // collection to store traversed inodes
  private final MongoCollection<Document> collection;
  // leaf nodes that can be inserted directly
  private ArrayList<INode> leaves;
  // current inode
  private INode current;

  public NodeTreeVisitor(MongoCollection<Document> collection) {
    this.collection = collection;
    this.current = null;
    this.leaves = new ArrayList<INode>();
  }

  protected INode getCurrent() {
    return this.current;
  }

  /**
   * Logic to update current node based on provided child node.
   * Provided child node should be treated as readonly.
   */
  private void updateCurrent(INode child) {
    this.current.addDiskUsage(child.getDiskUsage());
  }

  /** Convert file status into INode */
  private INode statusToNode(FileStatus root) {
    if (root.isDirectory()) {
      return new INode(root, INodeType.DIRECTORY);
    } else if (root.isFile()) {
      return new INode(root, INodeType.FILE);
    } else if (root.isSymlink()) {
      return new INode(root, INodeType.SYMLINK);
    } else {
      throw new UnsupportedOperationException("Unknown " + root);
    }
  }

  @Override
  public void visitBefore(FileStatus root) {
    this.current = statusToNode(root);
  }

  @Override
  public void visitChild(FileStatus child) {
    INode leaf = statusToNode(child);
    updateCurrent(leaf);
    this.leaves.add(leaf);
  }

  @Override
  public void visitChild(TreeVisitor visitor) {
    if (visitor instanceof NodeTreeVisitor) {
      NodeTreeVisitor nodeVisitor = (NodeTreeVisitor) visitor;
      // make parent <- child updates for current visitor
      INode child = nodeVisitor.getCurrent();
      updateCurrent(child);
    }
  }

  @Override
  public void visitAfter() {
    // insert leaves, if any exists, Mongo does not allow to insert empty lists
    if (this.leaves.size() > 0) {
      ArrayList<Document> leafDocs = new ArrayList<Document>(this.leaves.size());
      for (INode leaf : this.leaves) {
        leafDocs.add(leaf.toDocument());
      }
      collection.insertMany(leafDocs);
    }
    // insert current node
    collection.insertOne(this.current.toDocument());
    // clear all children and leaves
    this.leaves = null;
  }
}
