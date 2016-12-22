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

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OVarIntSerializer;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
public abstract class OAbstractPageWALRecord extends OOperationUnitBodyRecord {
  private long pageIndex;
  private int  fileId;

  protected OAbstractPageWALRecord() {
  }

  protected OAbstractPageWALRecord(long pageIndex, int fileId, OOperationUnitId operationUnitId) {
    super(operationUnitId);
    this.pageIndex = pageIndex;
    this.fileId = fileId;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    offset = super.toStream(content, offset);

    offset = OVarIntSerializer.writeUnsignedLong(pageIndex, content, offset);
    offset = OVarIntSerializer.writeUnsignedLong(fileId, content, offset);

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    offset = super.fromStream(content, offset);

    long[] res = OVarIntSerializer.readUnsignedLong(content, offset);
    pageIndex = res[0];
    offset = (int) res[1];

    int[] fres = OVarIntSerializer.readUnsignedInt(content, offset);
    fileId = fres[0];
    offset = fres[1];

    return offset;
  }

  @Override
  public int serializedSize() {
    return super.serializedSize() + OVarIntSerializer.computeUnsignedIntSize(fileId) + OVarIntSerializer
        .computeUnsignedLongSize(pageIndex);
  }

  public long getPageIndex() {
    return pageIndex;
  }

  public int getFileId() {
    return fileId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;

    OAbstractPageWALRecord that = (OAbstractPageWALRecord) o;

    if (fileId != that.fileId)
      return false;
    if (pageIndex != that.pageIndex)
      return false;
    if (lsn != null ? !lsn.equals(that.lsn) : that.lsn != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (lsn != null ? lsn.hashCode() : 0);
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + (int) (fileId ^ (fileId >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return toString("pageIndex=" + pageIndex + ", fileId=" + fileId);
  }
}
