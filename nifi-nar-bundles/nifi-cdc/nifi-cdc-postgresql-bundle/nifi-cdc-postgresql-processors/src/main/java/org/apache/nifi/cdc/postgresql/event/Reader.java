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
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

/**
 * Reader is the helper class for PostgreSQL Replication Stream, witch allow to
 * read event changes (INSERT, UPDATE, DELENTE, etc.) from a specific
 * Publication via a Replication Slot in real-time. It is possible to indicate a
 * position to start (LSN) reading at the object creation.
 *
 * @see org.apache.nifi.cdc.postgresql.event.Slot
 * @see org.apache.nifi.cdc.postgresql.event.Decode
 */
public class Reader {
    private boolean includeBeginCommit;
    private boolean includeAllMetadata;
    private String publication;
    private Slot replicationSlot;
    private PGReplicationStream replicationStream;

    public Reader(String slot, boolean dropSlotIfExists, String publication, Long lsn, boolean includeBeginCommit,
            boolean includeAllMetadata, Connection replicationConn, Connection queryConn)
            throws SQLException {

        try {
            // Creating replication slot.
            if (this.replicationSlot == null)
                this.replicationSlot = new Slot(slot, dropSlotIfExists, replicationConn, queryConn);
        } catch (SQLException e) {
            throw new SQLException("Failed to create replication slot. " + e, e);
        }

        try {
            // Creating replication stream.
            this.publication = publication;
            PGConnection pgConn = replicationConn.unwrap(PGConnection.class);
            if (lsn == null) {
                this.replicationStream = pgConn.getReplicationAPI().replicationStream().logical()
                        .withSlotName(this.replicationSlot.getName()).withSlotOption("proto_version", "1")
                        .withSlotOption("publication_names", this.publication).withStatusInterval(1, TimeUnit.SECONDS)
                        .start();
            } else {
                // Reading from LSN start position.
                LogSequenceNumber startLSN = LogSequenceNumber.valueOf(lsn);
                this.replicationStream = pgConn.getReplicationAPI().replicationStream().logical()
                        .withSlotName(this.replicationSlot.getName()).withSlotOption("proto_version", "1")
                        .withSlotOption("publication_names", this.publication).withStatusInterval(1, TimeUnit.SECONDS)
                        .withStartPosition(startLSN).start();
            }
        } catch (SQLException e) {
            throw new SQLException("Failed to create replication stream. " + e.getMessage(), e);
        }

        try {
            // Loading data types.
            if (Decode.isDataTypesEmpty())
                Decode.loadDataTypes(queryConn);

        } catch (SQLException e) {
            throw new SQLException("Failed to load data types. " + e.getMessage(), e);
        }

        this.includeBeginCommit = includeBeginCommit;
        this.includeAllMetadata = includeAllMetadata;
    }

    /**
     * Constructor only for unit tests.
     *
     * @see org.apache.nifi.cdc.postgresql.event.MockReader
     */
    public Reader(Long lsn, boolean includeBeginCommit, boolean includeAllMetadata) {
    }

    /**
     * Reads a pending event from the slot replication since the last
     * feedback. If received buffer is null, there is not more pending events. The
     * binary event buffer is decoded and converted to a JSON String.
     *
     * @return HashMap (String, Object)
     * @throws SQLException
     *                      if read event failed
     * @throws IOException
     *                      if decode event failed
     *
     * @see org.apache.nifi.cdc.postgresql.event.Slot
     * @see org.apache.nifi.cdc.postgresql.event.Decode
     */
    public HashMap<String, Object> readMessage() throws SQLException, IOException {
        ByteBuffer buffer;
        try {
            buffer = this.replicationStream.readPending();
        } catch (Exception e) {
            throw new SQLException("Failed to read pending events. " + e.getMessage(), e);
        }

        if (buffer == null)
            return null;

        // Binary buffer decode process.
        HashMap<String, Object> message;
        try {
            message = Decode.decodeLogicalReplicationBuffer(buffer, this.includeBeginCommit,
                    this.includeAllMetadata);
        } catch (Exception e) {
            throw new IOException("Failed to decode event buffer. " + e.getMessage(), e);
        }

        // Messages not requested/included.
        if (message.isEmpty())
            return message;

        LogSequenceNumber lsn = this.replicationStream.getLastReceiveLSN();
        Long lsnLong = lsn.asLong();

        // Some messagens don't have LSN (e.g. Relation).
        if (lsnLong > 0)
            message.put("lsn", lsnLong);

        return message;
    }

    /**
     * Converts event message to JSON String.
     *
     * @param message
     *                The key-value message.
     * @return String
     * @throws IOException
     *                     if convert message failed
     */
    public String convertMessageToJSON(HashMap<String, Object> message) throws IOException {
        ObjectMapper jsonMapper = new ObjectMapper();
        try {
            return jsonMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IOException("Failed to convert event message to JSON. " + e.getMessage(), e);
        }
    }

    /**
     * Returns the last received LSN from slot replication.
     *
     * @return Long
     */
    public Long getLastReceiveLSN() {
        return this.replicationStream.getLastReceiveLSN().asLong();
    }

    /**
     * Sends replication feedback to the database server.
     *
     * @param lsnLong
     *                Last received LSN as Long.
     */
    public void sendFeedback(Long lsnLong) {
        if (lsnLong > 0) {
            LogSequenceNumber lsn = LogSequenceNumber.valueOf(lsnLong);
            this.replicationStream.setAppliedLSN(lsn);
            this.replicationStream.setFlushedLSN(lsn);
        }
    }

    /**
     * Returns the Replication Slot used by this Replication Stream.
     *
     * @return Slot
     *
     * @see org.apache.nifi.cdc.postgresql.event.Slot
     */
    public Slot getReplicationSlot() {
        return replicationSlot;
    }

    /**
     * Sets a Replication Slot to this Replication Stream.
     *
     * @param replicationSlot
     *                        A Replication Slot.
     *
     * @see org.apache.nifi.cdc.postgresql.event.Slot
     */
    public void setReplicationSlot(Slot replicationSlot) {
        this.replicationSlot = replicationSlot;
    }

    /**
     * Returns the Replication Stream used to read events.
     *
     * @return PGReplicationStream
     */
    public PGReplicationStream getReplicationStream() {
        return this.replicationStream;
    }

    /**
     * Sets a Replication Stream to read events.
     *
     * @param replicationStream
     *                          A Replication Stream.
     */
    public void setReplicationStream(PGReplicationStream replicationStream) {
        this.replicationStream = replicationStream;
    }
}