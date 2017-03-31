package com.github.lightcopy.codec;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;

/**
 * Representation of tree path for inode.
 */
public class INodePath {
  // field names for document
  public static final String FIELD_DEPTH = "depth";
  // field name for each path level
  public static String FIELD_NAME(int index) { return "" + index; }

  // elements that construct path
  private String[] elements;
  private int depth;

  public INodePath(String path) {
    this(new Path(path));
  }

  public INodePath(Path path) {
    if (!path.isAbsolute()) {
      throw new IllegalArgumentException("Absolute path required, found " + path);
    }
    // path is always filled from left to right, leaving unused fields as nulls
    this.depth = path.depth();
    this.elements = new String[this.depth];
    int total = this.depth;
    while (total > 0) {
      this.elements[--total] = path.getName();
      path = path.getParent();
    }
  }

  protected INodePath(int depth, String[] elements) {
    if (depth < 0) {
      throw new IllegalArgumentException("Expected non-negative depth, found " + depth + " depth");
    }
    if (elements.length != depth) {
      throw new IllegalArgumentException("Inconsistent set of elements, " + elements.length +
        " != " + depth);
    }
    this.depth = depth;
    this.elements = elements;
  }

  /** Get current path depth */
  public int getDepth() {
    return this.depth;
  }

  /** Get mapped elements for the path */
  public Map<String, String> getElements() {
    Map<String, String> map = new LinkedHashMap<String, String>();
    for (int i = 0; i < this.depth; i++) {
      map.put(FIELD_NAME(i), this.elements[i]);
    }
    return map;
  }

  @Override
  public String toString() {
    return "Path" + Arrays.toString(this.elements) + "(" + this.depth + ")";
  }
}
