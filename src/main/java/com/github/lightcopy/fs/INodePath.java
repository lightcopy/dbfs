package com.github.lightcopy.fs;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import org.bson.Document;

import com.github.lightcopy.mongo.DocumentLike;

/**
 * Representation of tree path for inode.
 * Currently has a limitation on maximum number of levels (depth of the tree).
 */
public class INodePath implements DocumentLike<INodePath> {
  // field names for document
  public static final String FIELD_DEPTH = "depth";
  // field name for each path level
  private static String fieldName(int index) {
    return "" + index;
  }

  // elements that construct path
  private String[] elements;
  private int depth;

  public INodePath(Path path) {
    // path is always filled from left to right, leaving unused fields as nulls
    this.depth = path.depth();
    this.elements = parse(path);
  }

  /** Constructor to create from Document */
  public INodePath() { }

  protected String[] parse(Path path) {
    int total = path.depth();
    String[] elems = new String[total];
    while (total > 0) {
      elems[--total] = path.getName();
      path = path.getParent();
    }
    return elems;
  }

  /** Get current path depth */
  public int getDepth() {
    return this.depth;
  }

  /** Get mapped fields */
  public Map<String, String> getElements() {
    Map<String, String> map = new HashMap<String, String>();
    for (int i = 0; i < this.depth; i++) {
      map.put(fieldName(i), this.elements[i]);
    }
    return map;
  }

  /** Get element of the path for index */
  public String getElem(int index) {
    return this.elements[index];
  }

  /** Get ith element path */
  private Path getPath(int index) {
    if (index >= this.depth) return null;
    Path curr = new Path(this.elements[index]);
    Path next = getPath(index + 1);
    if (next == null) return curr;
    return new Path(curr, next);
  }

  /** Get path from collected elements, mainly for testing */
  public Path getPath() {
    Path path = getPath(0);
    if (path == null) return new Path(Path.SEPARATOR);
    return new Path(Path.SEPARATOR, getPath(0));
  }

  @Override
  public Document toDocument() {
    Document doc = new Document(FIELD_DEPTH, this.depth);
    for (int i = 0; i < this.depth; i++) {
      doc.append(fieldName(i), this.elements[i]);
    }
    return doc;
  }

  @Override
  public INodePath fromDocument(Document doc) {
    this.depth = doc.getInteger(FIELD_DEPTH);
    this.elements = new String[this.depth];
    for (int i = 0; i < this.depth; i++) {
      this.elements[i] = doc.getString(fieldName(i));
      if (i < this.depth && this.elements[i] == null) {
        throw new RuntimeException("Path element is null for depth " + this.depth +
          " and document " + doc);
      }
    }
    return this;
  }
}
