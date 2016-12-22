/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 5/21/14
 */
public class OFileCreatedWALRecord extends OOperationUnitBodyRecord {
  private String fileName;
  private int    fileId;

  public OFileCreatedWALRecord() {
  }

  public OFileCreatedWALRecord(OOperationUnitId operationUnitId, String fileName, int fileId) {
    super(operationUnitId);
    this.fileName = fileName;
    this.fileId = fileId;
  }

  public String getFileName() {
    return fileName;
  }

  public int getFileId() {
    return fileId;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    OStringSerializer.INSTANCE.serializeNativeObject(fileName, content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(fileName);

    offset = OVarIntSerializer.writeUnsignedLong(fileId, content, offset);

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    fileName = OStringSerializer.INSTANCE.deserializeNativeObject(content, offset);
    offset += OStringSerializer.INSTANCE.getObjectSize(fileName);

    int[] res = OVarIntSerializer.readUnsignedInt(content, offset);
    fileId = res[0];
    offset = res[1];

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OStringSerializer.INSTANCE.getObjectSize(fileName) +
        OVarIntSerializer.computeUnsignedIntSize(fileId);
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return false;
  }
}
