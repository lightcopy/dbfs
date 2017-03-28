package com.github.lightcopy.mongo;

import org.bson.Document;

/**
 * [[DocumentLike]] interface provides methods to convert instance into Mongo Document type and
 * vice versa.
 */
public interface DocumentLike<T> {
  /**
   * Convert current instance T into document.
   */
  public Document toDocument();

  /**
   * Convert provided document into instance T, this can either update existing instance, or
   * return brand-new object for that document.
   */
  public T fromDocument(Document doc);
}
