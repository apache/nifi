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

package org.apache.nifi.cdc.postgresql.pgeasyreplication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.fluent.logical.ChainedLogicalStreamBuilder;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;


/* Streams are data changes captured (BEGIN, COMMIT, INSERT, UPDATE, DELETE, etc.) via Slot Replication API and decoded. */
public class Stream {

    private final PGReplicationStream repStream;
    private final ConnectionManager connectionManager;
    private final ObjectMapper jsonObjectMapper = new ObjectMapper();
    private Decode decode;

    public static final String MIME_TYPE_OUTPUT_DEFAULT = "application/json";


    public Stream(String pub, String slot, ConnectionManager connectionManager, Long lsn) throws SQLException {
        Objects.requireNonNull(pub);
        Objects.requireNonNull(slot);
        this.connectionManager = Objects.requireNonNull(connectionManager);
        PGConnection pgcon = connectionManager.getReplicationConnection().unwrap(PGConnection.class);

        ChainedLogicalStreamBuilder repStreamBuilder = pgcon.getReplicationAPI()
                .replicationStream()
                .logical()
                .withSlotName(slot)
                .withSlotOption("proto_version", "1")
                .withSlotOption("publication_names", pub)
                .withStatusInterval(1, TimeUnit.SECONDS);

        if (lsn != null) {
            // Reading from LSN start position
            LogSequenceNumber startLSN = LogSequenceNumber.valueOf(lsn);
            repStreamBuilder.withStartPosition(startLSN);
        }

        repStream = repStreamBuilder.start();
    }

    public Event readStream(boolean isSimpleEvent, boolean withBeginCommit, String outputFormat)
            throws SQLException, InterruptedException, ParseException, UnsupportedEncodingException, JsonProcessingException {

        List<String> messages = new LinkedList<>();

        if (this.decode == null) {
            // First read stream
            decode = new Decode();
            decode.loadDataTypes(connectionManager);
        }

        while (true) {
            ByteBuffer buffer = repStream.readPending();

            if (buffer == null) {
                break;
            }

            Map<String, Object> message;

            if (isSimpleEvent) {
                message = decode.decodeLogicalReplicationMessageSimple(buffer, withBeginCommit);
            } else {
                message = decode.decodeLogicalReplicationMessage(buffer, withBeginCommit);
            }

            if (!message.isEmpty()) { // Skip empty messages
                messages.add(this.convertMessage(message, outputFormat.trim().toLowerCase()));
            }

            // Replication feedback
            repStream.setAppliedLSN(repStream.getLastReceiveLSN());
            repStream.setFlushedLSN(repStream.getLastReceiveLSN());
        }

        Long lastReceiveLSN = repStream.getLastReceiveLSN().asLong();

        return new Event(messages, lastReceiveLSN, isSimpleEvent, withBeginCommit, false);
    }

    public String convertMessage(Map<String, Object> message, String outputFormat) throws JsonProcessingException {
        switch (outputFormat) {
            case MIME_TYPE_OUTPUT_DEFAULT:
                return jsonObjectMapper.writeValueAsString(message);
            default:
                throw new IllegalArgumentException("Invalid output format:" + outputFormat);
        }
    }

}