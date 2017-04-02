package com.github.lightcopy.fs;

import java.util.Map;

import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;

/**
 * Custom INode conversion codec for Mongo.
 */
public class INodeCodec extends AbstractCodec<INode> {
  public INodeCodec() { }

  @Override
  public INode decode(BsonReader reader, DecoderContext decoderContext) {
    INode node = new INode();
    reader.readStartDocument();
    while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
      switch (reader.readName()) {
        case INode.FIELD_ACCESS_TIME:
          node.setAccessTime(reader.readInt64());
          break;
        case INode.FIELD_MODIFICATION_TIME:
          node.setModificationTime(reader.readInt64());
          break;
        case INode.FIELD_SIZE_BYTES:
          node.setSize(reader.readInt64());
          break;
        case INode.FIELD_BLOCK_SIZE_BYTES:
          node.setBlockSize(reader.readInt64());
          break;
        case INode.FIELD_REPLICATION_FACTOR:
          node.setReplicationFactor(reader.readInt32());
          break;
        case INode.FIELD_GROUP:
          node.setGroup(reader.readString());
          break;
        case INode.FIELD_OWNER:
          node.setOwner(reader.readString());
          break;
        case INode.FIELD_PERMISSION:
          node.setPermission(reader.readString());
          break;
        case INode.FIELD_NAME:
          node.setName(reader.readString());
          break;
        case INode.FIELD_PATH:
          // == path ==
          // structure and order of the fields is forced
          reader.readStartDocument();
          int depth = reader.readInt32(INodePath.FIELD_DEPTH);
          String[] elements = new String[depth];
          for (int i = 0; i < depth; i++) {
            elements[i] = reader.readString(INodePath.FIELD_NAME(i));
          }
          reader.readEndDocument();
          node.setPath(new INodePath(depth, elements));
          break;
        case INode.FIELD_TYPE:
          node.setTypeName(reader.readString());
          break;
        default:
          // ignore any other fields, e.g. object id
          reader.skipValue();
          break;
      }
    }
    reader.readEndDocument();
    // reconstruct inode
    return node;
  }

  @Override
  public Class<INode> getEncoderClass() {
    return INode.class;
  }

  @Override
  public void encode(BsonWriter writer, INode value, EncoderContext encoderContext) {
    writer.writeStartDocument();
    writer.writeInt64(INode.FIELD_ACCESS_TIME, value.getAccessTime());
    writer.writeInt64(INode.FIELD_MODIFICATION_TIME, value.getModificationTime());
    writer.writeInt64(INode.FIELD_SIZE_BYTES, value.getSize());
    writer.writeInt64(INode.FIELD_BLOCK_SIZE_BYTES, value.getBlockSize());
    writer.writeInt32(INode.FIELD_REPLICATION_FACTOR, value.getReplicationFactor());
    safeWriteString(writer, INode.FIELD_GROUP, value.getGroup());
    safeWriteString(writer, INode.FIELD_OWNER, value.getOwner());
    safeWriteString(writer, INode.FIELD_PERMISSION, value.getPermission());
    safeWriteString(writer, INode.FIELD_NAME, value.getName());
    // == path ==
    writer.writeName(INode.FIELD_PATH);
    writer.writeStartDocument();
    INodePath path = value.getPath();
    writer.writeInt32(INodePath.FIELD_DEPTH, path.getDepth());
    Map<String, String> map = path.getElements();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      safeWriteString(writer, entry.getKey(), entry.getValue());
    }
    writer.writeEndDocument();
    // == path ==
    safeWriteString(writer, INode.FIELD_TYPE, value.getTypeName());
    writer.writeEndDocument();
  }
}
