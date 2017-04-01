package com.github.lightcopy.fs;

import org.apache.hadoop.hdfs.inotify.Event;

/** [[EventContainer]] class is a container for HDFS event and transaction */
public class EventContainer {
  public static final String FIELD_TRANSACTION_ID = "transactionId";
  public static final String FIELD_EVENT_TYPE = "eventType";
  public static final String FIELD_EVENT = "event";
  public static final String FIELD_PATH = "path";
  public static final String FIELD_FILESIZE = "filesize";
  public static final String FIELD_TIMESTAMP = "timestamp";
  public static final String FIELD_CREATION_TIME = "creationTime";
  public static final String FIELD_GROUP = "group";
  public static final String FIELD_OWNER = "owner";
  public static final String FIELD_PERMISSION = "permission";
  public static final String FIELD_REPLICATION = "replication";
  public static final String FIELD_SYMLINK_TARGET = "symlinkTarget";
  public static final String FIELD_INODETYPE = "inodeType";
  public static final String FIELD_OVERWRITE = "overwrite";
  public static final String FIELD_SRC_PATH = "srcPath";
  public static final String FIELD_DST_PATH = "dstPath";
  public static final String FIELD_ACCESS_TIME = "accessTime";
  public static final String FIELD_MODIFICATION_TIME = "modificationTime";
  public static final String FIELD_METADATA_TYPE = "metadataType";

  private final Event event;
  private final long transactionId;

  public EventContainer(long transactionId, Event event) {
    this.event = event;
    this.transactionId = transactionId;
  }

  /** Get underlying event */
  public Event getEvent() {
    return this.event;
  }

  /** Get transaction id for event */
  public long getTransactionId() {
    return this.transactionId;
  }

  @Override
  public String toString() {
    return "TX[" + getTransactionId() + "]" + getEvent();
  }
}
