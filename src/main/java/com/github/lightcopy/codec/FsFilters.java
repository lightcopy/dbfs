package com.github.lightcopy.codec;

import java.util.ArrayList;
import java.util.Map;

import org.bson.Document;
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

  /** Generate filter to find all paths with depth of provided path */
  private static Bson depth(INodePath path) {
    return Filters.eq(INodePath.FIELD_DEPTH, path.getDepth());
  }

  /** Generate filter to combine all elements of the path */
  private static ArrayList<Bson> pathElements(INodePath path) {
    ArrayList<Bson> filters = new ArrayList<Bson>();
    Map<String, String> map = path.getElements();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      Bson elem = Filters.eq(entry.getKey(), entry.getValue());
      filters.add(elem);
    }
    return filters;
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
    return Filters.or(filters);
  }

  /** Print filter as json */
  public static String prettyString(Bson filter) {
    CodecRegistry registry = CodecRegistries.fromCodecs(
      new DocumentCodec(), new IntegerCodec(), new LongCodec(), new StringCodec());
    return filter.toBsonDocument(Document.class, registry).toString();
  }
}
