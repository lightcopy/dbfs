package com.github.lightcopy.fs;

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

  /** Get element by index, index must be in range (less than depth) */
  public String getElement(int index) {
    return this.elements[index];
  }

  /** Get underlying array for node path, should be read-only, does not return copy */
  public String[] array() {
    return this.elements;
  }

  /** Return first parent for this node or null, if it is root */
  public INodePath getParent() {
    if (this.depth == 0) return null;
    String[] parts = new String[this.depth - 1];
    System.arraycopy(this.elements, 0, parts, 0, this.depth - 1);
    return new INodePath(parts.length, parts);
  }

  /** Return last element if depth > 0, otherwise return empty string (root) */
  public String getName() {
    // in case of root directory
    if (this.depth == 0) return "";
    return this.elements[this.depth - 1];
  }

  /** Check if current path starts with prefix */
  public boolean hasPrefix(INodePath prefix) {
    if (this.depth < prefix.getDepth()) return false;
    for (int i = 0; i < prefix.getDepth(); i++) {
      if (!this.elements[i].equals(prefix.getElement(i))) {
        return false;
      }
    }
    return true;
  }

  /** Return new path with updated prefix */
  public INodePath withUpdatedPrefix(INodePath prefix, INodePath replacement) {
    if (!hasPrefix(prefix)) {
      throw new IllegalArgumentException("Prefix " + prefix + "is not prefix of the path " + this);
    }
    int total = this.depth - prefix.getDepth() + replacement.getDepth();
    String[] elems = new String[total];
    System.arraycopy(replacement.array(), 0, elems, 0, replacement.getDepth());
    System.arraycopy(this.elements, prefix.getDepth(), elems, replacement.getDepth(),
      this.depth - prefix.getDepth());
    return new INodePath(total, elems);
  }

  @Override
  public String toString() {
    return "Path" + Arrays.toString(this.elements) + "(" + this.depth + ")";
  }
}
