package com.github.lightcopy.fs;

import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.Event.AppendEvent;
import org.apache.hadoop.hdfs.inotify.Event.CloseEvent;
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent;
import org.apache.hadoop.hdfs.inotify.Event.MetadataUpdateEvent;
import org.apache.hadoop.hdfs.inotify.Event.RenameEvent;
import org.apache.hadoop.hdfs.inotify.Event.UnlinkEvent;
import org.apache.hadoop.hdfs.inotify.Event.EventType;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

/**
 * Custom EventContainer (HDFS event) conversion codec for Mongo.
 */
public class EventCodec implements Codec<EventContainer> {
  public EventCodec() { }

  @Override
  public EventContainer decode(BsonReader reader, DecoderContext decoderContext) {
    // TODO: implement decoding of events
    throw new UnsupportedOperationException("Decoding of events in unsupported");
  }

  @Override
  public Class<EventContainer> getEncoderClass() {
    return EventContainer.class;
  }

  @Override
  public void encode(BsonWriter writer, EventContainer value, EncoderContext encoderContext) {
    long transaction = value.getTransactionId();
    Event event = value.getEvent();

    writer.writeStartDocument();
    writer.writeInt64(EventContainer.FIELD_TRANSACTION_ID, transaction);
    writer.writeString(EventContainer.FIELD_EVENT_TYPE, event.getEventType().name());
    // == event ==
    writer.writeName(INode.FIELD_PATH);
    switch (event.getEventType()) {
      case APPEND:
        encode(writer, (AppendEvent) event);
        break;
      case CLOSE:
        encode(writer, (CloseEvent) event);
        break;
      case CREATE:
        encode(writer, (CreateEvent) event);
        break;
      case METADATA:
        encode(writer, (MetadataUpdateEvent) event);
        break;
      case RENAME:
        encode(writer, (RenameEvent) event);
        break;
      case UNLINK:
        encode(writer, (UnlinkEvent) event);
        break;
      default:
        throw new UnsupportedOperationException("Unrecognized event " + event);
    }
    // == event ==
    writer.writeEndDocument();
  }

  /**
   * Encode HDFS append events.
   * Sent when an existing file is opened for append.
   */
  private void encode(BsonWriter writer, AppendEvent event) {
    writer.writeStartDocument();
    writer.writeString(EventContainer.FIELD_PATH, event.getPath());
    writer.writeEndDocument();
  }

  /**
   * Encode HDFS close events.
   * Sent when a file is closed after append or create.
   */
  private void encode(BsonWriter writer, CloseEvent event) {
    writer.writeStartDocument();
    writer.writeInt64(EventContainer.FIELD_FILESIZE, event.getFileSize());
    writer.writeString(EventContainer.FIELD_PATH, event.getPath());
    writer.writeInt64(EventContainer.FIELD_TIMESTAMP, event.getTimestamp());
    writer.writeEndDocument();
  }

  /**
   * Encode HDFS create events.
   * Sent when a new file is created (including overwrite).
   */
  private void encode(BsonWriter writer, CreateEvent event) {
    writer.writeStartDocument();
    writer.writeInt64(EventContainer.FIELD_CREATION_TIME, event.getCtime());
    writer.writeString(EventContainer.FIELD_GROUP, event.getGroupName());
    writer.writeString(EventContainer.FIELD_INODETYPE, event.getiNodeType().name());
    writer.writeBoolean(EventContainer.FIELD_OVERWRITE, event.getOverwrite());
    writer.writeString(EventContainer.FIELD_OWNER, event.getOwnerName());
    writer.writeString(EventContainer.FIELD_PATH, event.getPath());
    writer.writeString(EventContainer.FIELD_PERMISSION, event.getPerms().toString());
    writer.writeInt32(EventContainer.FIELD_REPLICATION, event.getReplication());
    writer.writeString(EventContainer.FIELD_SYMLINK_TARGET, event.getSymlinkTarget());
    writer.writeEndDocument();
  }

  /**
   * Encode HDFS metadata events.
   * Sent when there is an update to directory or file (none of the metadata tracked here applies
   * to symlinks) that is not associated with another inotify event. The tracked metadata includes
   * atime/mtime, replication, owner/group, permissions, ACLs, and XAttributes. Fields not relevant
   * to the metadataType of the MetadataUpdateEvent will be null or will have their default values.
   */
  private void encode(BsonWriter writer, MetadataUpdateEvent event) {
    // do not write Acls and xAttributes
    writer.writeStartDocument();
    writer.writeInt64(EventContainer.FIELD_ACCESS_TIME, event.getAtime());
    writer.writeString(EventContainer.FIELD_GROUP, event.getGroupName());
    writer.writeString(EventContainer.FIELD_METADATA_TYPE, event.getMetadataType().name());
    writer.writeInt64(EventContainer.FIELD_MODIFICATION_TIME, event.getMtime());
    writer.writeString(EventContainer.FIELD_OWNER, event.getOwnerName());
    writer.writeString(EventContainer.FIELD_PATH, event.getPath());
    writer.writeString(EventContainer.FIELD_PERMISSION, event.getPerms().toString());
    writer.writeInt32(EventContainer.FIELD_REPLICATION, event.getReplication());
    writer.writeEndDocument();
  }

  /**
   * Encode HDFS rename events.
   * Sent when a file, directory, or symlink is renamed.
   */
  private void encode(BsonWriter writer, RenameEvent event) {
    writer.writeStartDocument();
    writer.writeString(EventContainer.FIELD_SRC_PATH, event.getSrcPath());
    writer.writeString(EventContainer.FIELD_DST_PATH, event.getDstPath());
    writer.writeInt64(EventContainer.FIELD_TIMESTAMP, event.getTimestamp());
    writer.writeEndDocument();
  }

  /**
   * Encode HDFS unlink events.
   * Sent when a file, directory, or symlink is deleted.
   */
  private void encode(BsonWriter writer, UnlinkEvent event) {
    writer.writeStartDocument();
    writer.writeString(EventContainer.FIELD_PATH, event.getPath());
    writer.writeInt64(EventContainer.FIELD_TIMESTAMP, event.getTimestamp());
    writer.writeEndDocument();
  }
}
