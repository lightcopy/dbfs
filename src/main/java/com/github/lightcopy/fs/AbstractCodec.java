package com.github.lightcopy.fs;

import org.apache.hadoop.fs.permission.FsPermission;

import org.bson.BsonWriter;
import org.bson.codecs.Codec;

/**
 * [[AbstractCodec]] provides some common functionality to read/write bson documents.
 * As part of codec interface subclasses must override:
 * - decode(reader, decoderContext)
 * - getEncoderClass()
 * - encode(writer, value, encoderContext)
 * See org.bson.codecs.Codec interface for more information.
 */
public abstract class AbstractCodec<T> implements Codec<T> {

  /**
   * Method to write String safely bypassing null values.
   * @param writer bson writer to use
   * @param name key
   * @param value string value or null
   */
  public void safeWriteString(BsonWriter writer, String name, String value) {
    if (value == null) {
      writer.writeNull(name);
    } else {
      writer.writeString(name, value);
    }
  }

  /**
   * Write permission object safely considering null value. Writer will store string representation
   * of FsPermission object if instance is not null.
   * @param writer bson writer to use
   * @param name key
   * @param perm FsPermission object or null
   */
  public void safeWritePermString(BsonWriter writer, String name, FsPermission perm) {
    if (perm == null) {
      writer.writeNull(name);
    } else {
      writer.writeString(name, perm.toString());
    }
  }
}
