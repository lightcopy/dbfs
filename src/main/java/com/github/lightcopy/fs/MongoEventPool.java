package com.github.lightcopy.fs;

import com.mongodb.client.MongoCollection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mongo event pool to store all hdfs events that are captured by [[EventProcess]].
 */
public class MongoEventPool {
  private static final Logger LOG = LoggerFactory.getLogger(MongoEventPool.class);

  public MongoEventPool(MongoCollection<?> collection) { }
}
