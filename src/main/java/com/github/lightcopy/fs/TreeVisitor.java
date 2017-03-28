package com.github.lightcopy.fs;

import org.apache.hadoop.fs.FileStatus;

/**
 * Interface to visit file system, represents traversing directory. All files, symlinks, etc. will
 * be called as 'visitChild' with file status. Access to lower level of directories can be
 * implemented using method `visitChild(TreeVisitor prev)`.
 */
public interface TreeVisitor {
  /**
   * Process current root directory (for this level) before actual listing starts. Root directory
   * is a directory that represents current visitor.
   */
  public void visitBefore(FileStatus root);

  /**
   * Process child of current level root directory. Child is not a directory, it is a file or
   * symlink, etc. Leaf node that cannot be traversed further to extract information.
   */
  public void visitChild(FileStatus child);

  /**
   * Process visitor for previous child level, represents child directory. At this point, child
   * visitor has already been collected and all TreeVisitor methods have been called. Use this
   * method to update statistics for root directory based on child directory of visitor.
   */
  public void visitChild(TreeVisitor visitor);

  /**
   * Process current directory after all children for this level have been processed. For example,
   * implementation might provide saving collected information into some sink.
   */
  public void visitAfter();
}
