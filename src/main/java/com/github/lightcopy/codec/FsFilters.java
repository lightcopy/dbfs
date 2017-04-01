package com.github.lightcopy.codec;

import java.util.ArrayList;
import java.util.Map;

import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.IntegerCodec;
import org.bson.codecs.LongCodec;
import org.bson.codecs.StringCodec;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import com.mongodb.client.model.Filters;

/**
 * Filters class provides static methods to generate filters for inode path.
 */
public class FsFilters {
  private FsFilters() { }

  /** Return dot separated key, currently is only supported for inode path */
  private static String pathKey(String field) {
    return INode.FIELD_PATH + "." + field;
  }

  /** Generate filter to find all paths with depth of provided path */
  private static Bson depth(INodePath path) {
    return Filters.eq(pathKey(INodePath.FIELD_DEPTH), path.getDepth());
  }

  /** Generate filter to combine all elements of the path */
  private static ArrayList<Bson> pathElements(INodePath path) {
    ArrayList<Bson> filters = new ArrayList<Bson>();
    Map<String, String> map = path.getElements();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      Bson elem = Filters.eq(pathKey(entry.getKey()), entry.getValue());
      filters.add(elem);
    }
    return filters;
  }

  /**
   * Filter to indicate that we need to fetch all nodes in file system. It is only used as
   * placeholder instead of returning null for situations when deleting root directory or renaming
   * root directory.
   */
  private static Bson all() {
    return Filters.exists(INode.FIELD_PATH);
  }

  /**
   * Filter to indicate that we do not need to return any nodes in file system. It is only used as
   * placeholder instead of returning null for operations with root directory.
   */
  private static Bson none() {
    return Filters.not(all());
  }

  /** Generate filter to find particular path */
  public static Bson path(INodePath path) {
    ArrayList<Bson> filters = pathElements(path);
    filters.add(depth(path));
    return Filters.and(filters);
  }

  /**
   * Generate filter to find all paths that contain the same elements as provided path. Used to
   * search for itself and all its children recursively.
   */
  public static Bson paths(INodePath path) {
    ArrayList<Bson> filters = pathElements(path);
    // empty filter list indicates root directory, and all dhild nodes should be selected
    if (filters.isEmpty()) return all();
    return Filters.and(filters);
  }

  /** Generate filters to fetch parent nodes for this path. Does not return itself */
  public static Bson parentPaths(INodePath path) {
    ArrayList<Bson> filters = new ArrayList<Bson>();
    INodePath parent = path.getParent();
    while (parent != null) {
      filters.add(path(parent));
      parent = parent.getParent();
    }
    // empty list indicates root directory that does not have any parent nodes
    if (filters.isEmpty()) return none();
    return Filters.or(filters);
  }

  /** Print filter as json */
  public static String prettyString(Bson filter) {
    CodecRegistry registry = CodecRegistries.fromCodecs(new BsonDocumentCodec(),
      new DocumentCodec(), new IntegerCodec(), new LongCodec(), new StringCodec());
    return filter.toBsonDocument(Document.class, registry).toString();
  }
}
