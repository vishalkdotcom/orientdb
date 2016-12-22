package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;

public class OFileDeletedWALRecord extends OOperationUnitBodyRecord {
  private int fileId;

  public OFileDeletedWALRecord() {
  }

  public OFileDeletedWALRecord(OOperationUnitId operationUnitId, int fileId) {
    super(operationUnitId);
    this.fileId = fileId;
  }

  public int getFileId() {
    return fileId;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    offset = OVarIntSerializer.writeUnsignedLong(fileId, content, offset);

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    int[] res = OVarIntSerializer.readUnsignedInt(content, offset);
    fileId = res[0];
    offset = res[1];

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OVarIntSerializer.computeUnsignedIntSize(fileId);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }
}
