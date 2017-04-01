package com.github.lightcopy.fs;

import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import com.mongodb.client.MongoCollection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mongo event pool to store all hdfs events that are captured by [[EventProcess]].
 */
public class MongoEventPool {
  private static final Logger LOG = LoggerFactory.getLogger(MongoEventPool.class);

  /** Mongo event pool collection */
  private final MongoCollection<EventContainer> pool;

  public MongoEventPool(MongoCollection<?> collection) {
    CodecRegistry defaults = collection.getCodecRegistry();
    CodecRegistry support = CodecRegistries.fromCodecs(new EventCodec());
    this.pool = collection
      .withCodecRegistry(CodecRegistries.fromRegistries(defaults, support))
      .withDocumentClass(EventContainer.class);
  }

  /** Insert single event container */
  public void insert(EventContainer container) {
    this.pool.insertOne(container);
    LOG.info("Added event {}", container);
  }
}
