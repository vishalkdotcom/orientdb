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
 * @since 14.05.13
 */
public abstract class OAbstractCheckPointStartRecord extends OAbstractWALRecord {
  private OLogSequenceNumber previousCheckpoint;

  protected OAbstractCheckPointStartRecord() {
  }

  protected OAbstractCheckPointStartRecord(OLogSequenceNumber previousCheckpoint) {
    this.previousCheckpoint = previousCheckpoint;
  }

  public OLogSequenceNumber getPreviousCheckpoint() {
    return previousCheckpoint;
  }

  @Override
  public int toStream(byte[] content, int offset) {
    if (previousCheckpoint == null) {
      content[offset] = 0;
      offset++;
      return offset;
    }

    content[offset] = 1;
    offset++;

    offset = OVarIntSerializer.writeUnsignedLong(previousCheckpoint.getSegment(), content, offset);
    offset = OVarIntSerializer.writeUnsignedLong(previousCheckpoint.getPosition(), content, offset);

    return offset;
  }

  @Override
  public int fromStream(byte[] content, int offset) {
    if (content[offset] == 0) {
      offset++;
      return offset;
    }

    offset++;

    long[] res = OVarIntSerializer.readUnsignedLong(content, offset);
    long segment = res[0];
    offset = (int) res[1];

    res = OVarIntSerializer.readUnsignedLong(content, offset);
    long position = res[0];
    offset = (int) res[1];

    previousCheckpoint = new OLogSequenceNumber(segment, position);

    return offset;
  }

  @Override
  public int serializedSize() {
    if (previousCheckpoint == null)
      return 1;

    return OVarIntSerializer.computeUnsignedLongSize(previousCheckpoint.getSegment()) + OVarIntSerializer
        .computeUnsignedLongSize(previousCheckpoint.getPosition()) + 1;
  }

  @Override
  public boolean isUpdateMasterRecord() {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OAbstractCheckPointStartRecord that = (OAbstractCheckPointStartRecord) o;

    if (previousCheckpoint != null ? !previousCheckpoint.equals(that.previousCheckpoint) : that.previousCheckpoint != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return previousCheckpoint != null ? previousCheckpoint.hashCode() : 0;
  }

  @Override
  public String toString() {
    return toString("previousCheckpoint=" + previousCheckpoint);
  }
}
