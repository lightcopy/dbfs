package com.github.lightcopy.mongo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;

import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import com.github.lightcopy.fs.INode;
import com.github.lightcopy.fs.INodePath;

/**
 * [[MongoUtils]] to deal with different file system operations on mongo collection.
 */
public class MongoUtils {
  /** Create composite key in format "a.b.c" */
  private static String compositeKey(String... components) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < components.length; i++) {
      sb.append(components[i]);
      if (i < components.length - 1) {
        sb.append(".");
      }
    }
    return sb.toString();
  }

  /**
   * Create filter to find exact path. If exact search is true, then it will create filter than
   * matches path and all subpaths (children).
   */
  private static Bson filterPath(INodePath path, boolean exact) {
    ArrayList<Bson> filters = new ArrayList<Bson>();
    // add filter on depth
    if (exact) {
      filters.add(Filters.eq(compositeKey(INode.FIELD_PATH,
        INodePath.FIELD_DEPTH), path.getDepth()));
    }
    // add filters on path components
    Map<String, String> elems = path.getElements();
    for (String key : elems.keySet()) {
      filters.add(Filters.eq(compositeKey(INode.FIELD_PATH, key), elems.get(key)));
    }
    return Filters.and(filters);
  }

  /**
   * Find inode in file system collection based on provided path. If no such file exists, null is
   * returned.
   */
  public static INode find(MongoCollection<Document> fs, INodePath path) {
    Document document = fs.find(filterPath(path, true)).first();
    if (document == null) return null;
    return new INode().fromDocument(document);
  }

  /**
   * Find all ndoes with prefix of path.
   */
  public static Iterator<INode> findAll(MongoCollection<Document> fs, INodePath path) {
    final MongoCursor<Document> cursor = fs.find(filterPath(path, false)).iterator();
    return new Iterator<INode>() {
      @Override
      public boolean hasNext() {
        boolean hasNext = cursor.hasNext();
        if (!hasNext) {
          cursor.close();
        }
        return hasNext;
      }

      @Override
      public INode next() {
        Document doc = cursor.next();
        return new INode().fromDocument(doc);
      }

      @Override
      public void remove() { }
    };
  }

  /**
   * Delete path recursively, every inode that starts with this path will be removed.
   */
  public static long deleteRecursively(MongoCollection<Document> fs, INodePath path) {
    // find all matching paths and subpaths
    DeleteResult result = fs.deleteMany(filterPath(path, false));
    return result.getDeletedCount();
  }

  /**
   * Find all parent paths for this provided path, does not include current path.
   */
  public static Iterator<INode> findParents(MongoCollection<Document> fs, INodePath path) {
    Path filepath = path.getPath();
    if (filepath == null || filepath.getParent() == null) {
      return Collections.emptyIterator();
    }

    ArrayList<Bson> filters = new ArrayList<Bson>();
    while (filepath != null) {
      INodePath parentPath = new INodePath(filepath);
      filters.add(filterPath(parentPath, true));
      filepath = filepath.getParent();
    }
    final MongoCursor<Document> cursor = fs.find(Filters.or(filters)).iterator();
    return new Iterator<INode>() {
      @Override
      public boolean hasNext() {
        boolean hasNext = cursor.hasNext();
        if (!hasNext) {
          cursor.close();
        }
        return hasNext;
      }

      @Override
      public INode next() {
        Document doc = cursor.next();
        return new INode().fromDocument(doc);
      }

      @Override
      public void remove() { }
    };
  }

  /**
   * Update single node in file system.
   */
  public static long update(MongoCollection<Document> fs, INode node) {
    Bson filter = Filters.eq(INode.FIELD_UUID, node.getUUID());
    UpdateResult res = fs.replaceOne(filter, node.toDocument());
    return res.getModifiedCount();
  }

  /**
   * Insert new node into file system.
   */
  public static void insert(MongoCollection<Document> fs, INode node) {
    fs.insertOne(node.toDocument());
  }
}
