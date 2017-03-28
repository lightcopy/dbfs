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
  // child nodes that have already been inserted but require updates
  private ArrayList<INode> children;
  // current inode
  private INode current;

  public NodeTreeVisitor(MongoCollection<Document> collection) {
    this.collection = collection;
    this.current = null;
    this.leaves = new ArrayList<INode>();
    this.children = new ArrayList<INode>();
  }

  protected INode getCurrent() {
    return this.current;
  }

  /** Logic to update both current node as well as provided child node */
  private void updateParentChild(INode child) {
    child.setParent(this.current);
    this.current.addDiskUsage(child.getDiskUsage());
  }

  /** Convert file status into INode */
  private INode statusToNode(FileStatus root) {
    if (root.isDirectory()) {
      return new DirectoryNode(root);
    } else if (root.isFile()) {
      return new FileNode(root);
    } else if (root.isSymlink()) {
      return new SymlinkNode(root);
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
    updateParentChild(leaf);
    this.leaves.add(leaf);
  }

  @Override
  public void visitChild(TreeVisitor visitor) {
    if (visitor instanceof NodeTreeVisitor) {
      NodeTreeVisitor nodeVisitor = (NodeTreeVisitor) visitor;
      // make parent-child updates for current visitor, because root of visitor is not passed
      // into visitChild(FileStatus), do not add root of visitor into children list
      INode child = nodeVisitor.getCurrent();
      updateParentChild(child);
      this.children.add(child);
    }
  }

  @Override
  public void visitAfter() {
    // store updated children
    for (INode child : this.children) {
      collection.replaceOne(Filters.eq(INode.UUID_COLUMN, child.getId()), child.toDocument());
    }
    // insert all leaves
    ArrayList<Document> leafDocs = new ArrayList<Document>(this.leaves.size());
    for (INode leaf : this.leaves) {
      leafDocs.add(leaf.toDocument());
    }
    collection.insertMany(leafDocs);
    // insert current node
    collection.insertOne(this.current.toDocument());
  }
}
