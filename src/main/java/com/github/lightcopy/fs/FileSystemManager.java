package com.github.lightcopy.fs;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * [[FileSystemManager]] is responsible for indexing HDFS file system, processing events from
 * HDFS stream, and maintaining connection to any external storage system.
 */
public abstract class FileSystemManager {
  /**
   * Prepare tree visitor for a directory. All initialization code should go into this method,
   * including allocating buffers for child leaves, etc. This method is invoked before walking
   * part of the tree.
   */
  public abstract TreeVisitor prepareTreeVisitor();

  /**
   * Get file system used by this file system manager.
   * Should return the same instance when called multiple times.
   */
  public abstract FileSystem getFileSystem();

  /**
   * Return root path for traversing managed by this file system manager. Must be visible for
   * current traversal and must be a directory. Method should be stable, and return the same path
   * when called multiple times.
   */
  public abstract Path getRoot();

  /**
   * Initialize manager, this should include buffering streams, creating connections, and file
   * system. Method is called only once.
   */
  public abstract void start();

  /**
   * Close associated resources, e.g. connection, event stream, etc.
   * Method is called only once.
   */
  public abstract void stop();

  /**
   * Traverse root directory and propagate visitor for indexing.
   */
  protected void indexFileSystem() throws IOException {
    FileSystem fs = getFileSystem();
    Path root = getRoot();
    FileStatus rootStatus = fs.getFileStatus(root);
    if (!rootStatus.isDirectory()) {
      throw new IllegalArgumentException("Expected root path as directory, got " + rootStatus);
    }
    TreeVisitor visitor = prepareTreeVisitor();
    walkTree(fs, rootStatus, visitor);
  }

  /**
   * Walk file system tree starting with root directory. Root must be a valid directory, otherwise
   * traversal is ignored, each file or symlink (non-directory) node is processed as child of
   * current tree traversal.
   */
  private void walkTree(FileSystem fs, FileStatus root, TreeVisitor visitor)
      throws FileNotFoundException, IOException {
    if (root.isDirectory()) {
      visitor.visitBefore(root);
      FileStatus[] children = fs.listStatus(root.getPath());
      if (children != null && children.length > 0) {
        for (FileStatus child : children) {
          if (child.isDirectory()) {
            TreeVisitor levelVisitor = prepareTreeVisitor();
            walkTree(fs, child, levelVisitor);
            visitor.visitChild(levelVisitor);
          } else {
            visitor.visitChild(child);
          }
        }
      }
      visitor.visitAfter();
    }
  }

  @Override
  public String toString() {
    return "[fs=" + getFileSystem() + ", root=" + getRoot() + "]";
  }
}
