/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.cdc.postgresql.event;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;

import org.postgresql.replication.LogSequenceNumber;

/**
 * Mock Reader class.
 */
public class MockReader extends Reader {
  private boolean includeBeginCommit;
  private boolean includeAllMetadata;
  private MockPGReplicationStream replicationStream;

  public MockReader(Long lsn, boolean includeBeginCommit, boolean includeAllMetadata) throws SQLException {
    super(lsn, includeBeginCommit, includeAllMetadata);

    try {
      // Creating a mock replication stream
      LogSequenceNumber startLSN = lsn == null ? LogSequenceNumber.valueOf(100L) : LogSequenceNumber.valueOf(lsn);
      this.replicationStream = new MockPGReplicationStream(startLSN);

    } catch (SQLException e) {
      throw new SQLException("Failed to create replication stream. " + e, e);
    }

    // Loading data types
    HashMap<Integer, String> dataTypes = new HashMap<Integer, String>();
    dataTypes.put(23, "int4");
    Decode.setDataTypes(dataTypes);

    this.includeBeginCommit = includeBeginCommit;
    this.includeAllMetadata = includeAllMetadata;
  }

  @Override
  public HashMap<String, Object> readMessage() throws IOException, SQLException {
    ByteBuffer buffer;
    try {
      buffer = this.replicationStream.readPending();
    } catch (UnsupportedEncodingException e) {
      throw new SQLException("Failed to read pending events. " + e.getMessage(), e);
    }

    if (buffer == null)
      return null;

    LogSequenceNumber lsn = this.replicationStream.getLastReceiveLSN();
    Long lsnLong = lsn.asLong();

    // Some messagens don't have LSN (e.g. Relation)
    if (lsnLong > 0) {
      // Replication feedback
      this.replicationStream.setAppliedLSN(lsn);
      this.replicationStream.setFlushedLSN(lsn);
    }

    // Binary buffer decode process
    HashMap<String, Object> message;
    try {
      message = Decode.decodeLogicalReplicationBuffer(buffer, this.includeBeginCommit,
          this.includeAllMetadata);
    } catch (UnsupportedEncodingException | ParseException e) {
      throw new IOException("Failed to decode event buffer. " + e.getMessage(), e);
    }

    // Messages not requested
    if (message.isEmpty())
      return message;

    // Some messagens don't have LSN (e.g. Relation)
    if (lsnLong > 0)
      message.put("lsn", lsnLong);

    return message;
  }

  @Override
  public Long getLastReceiveLSN() {
    return this.replicationStream.getLastReceiveLSN().asLong();
  }

  @Override
  public void sendFeedback(Long lsnLong) {
    if (lsnLong > 0) {
      LogSequenceNumber lsn = LogSequenceNumber.valueOf(lsnLong);
      this.replicationStream.setAppliedLSN(lsn);
      this.replicationStream.setFlushedLSN(lsn);
    }
  }

  /**
   * Mock PGReplicationStream class.
   */
  private class MockPGReplicationStream {
    private LogSequenceNumber lastReceiveLSN;
    private Long lsn;
    private int sequence = 1;

    private MockPGReplicationStream(LogSequenceNumber startLSN) throws SQLException {
      this.lastReceiveLSN = startLSN;
      this.lsn = startLSN.asLong();
    }

    private ByteBuffer readPending() throws UnsupportedEncodingException {
      ByteBuffer buffer = ByteBuffer.allocate(100);

      switch (this.sequence) {

        case 1: // BEGIN
          lsn++;
          buffer = buffer.put((byte) 'B'); // messageType
          buffer = buffer.putLong(lsn); // xLSNFinal
          buffer = buffer.putLong(0L); // xCommitTime
          buffer = buffer.putInt(14); // xid
          this.lastReceiveLSN = LogSequenceNumber.valueOf(lsn);
          break;

        case 2: // RELATION
          buffer = buffer.put((byte) 'R'); // messageType
          buffer = buffer.putInt(14); // relationId
          buffer = buffer.put("public".getBytes()); // relationNamespace
          buffer = buffer.put((byte) 0); // delimiter
          buffer = buffer.put("tb_city".getBytes()); // relationName
          buffer = buffer.put((byte) 0); // delimiter
          buffer = buffer.put((byte) 'f'); // replicaIdentity
          buffer = buffer.putShort((short) 1); // numberColumns
          buffer = buffer.put((byte) '1'); // isKey
          buffer = buffer.put("id".getBytes()); // columnnName
          buffer = buffer.put((byte) 0); // delimiter
          buffer = buffer.putInt(23); // dataTypeId
          buffer = buffer.putInt(-1); // typeModifier
          this.lastReceiveLSN = LogSequenceNumber.valueOf(-1L);
          break;

        case 3: // INSERT
          buffer = buffer.put((byte) 'I'); // messageType
          buffer = buffer.putInt(14); // relationId
          buffer = buffer.put((byte) 'N'); // tupleType
          buffer = buffer.putShort((short) 1); // numberColumns
          buffer = buffer.put((byte) 't'); // dataFormat
          buffer = buffer.putInt(1); // lengthColumnValue
          buffer = buffer.put("8".getBytes()); // columnValue
          this.lastReceiveLSN = LogSequenceNumber.valueOf(lsn);
          break;

        case 4: // COMMIT
          lsn++;
          buffer = buffer.put((byte) 'C'); // messageType
          buffer = buffer.put((byte) 0); // flags
          buffer = buffer.putLong(lsn); // commitLSN
          buffer = buffer.putLong(lsn); // xLSNEnd
          buffer = buffer.putLong(1000000L); // xCommitTime
          this.lastReceiveLSN = LogSequenceNumber.valueOf(lsn);
          break;

        default:
          lsn++;
          buffer = null; // No more pedding events.
          this.lastReceiveLSN = LogSequenceNumber.valueOf(lsn);
      }

      this.sequence++;
      return buffer;
    }

    private LogSequenceNumber getLastReceiveLSN() {
      return this.lastReceiveLSN;
    }

    private void setAppliedLSN(LogSequenceNumber lsn) {
    }

    private void setFlushedLSN(LogSequenceNumber lsn) {
    }
  }

}
