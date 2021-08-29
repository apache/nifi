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

package org.apache.nifi.cdc.postgresql.pgEasyReplication;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationStream;

import org.apache.nifi.cdc.postgresql.pgEasyReplication.ConnectionManager;

public class Stream { 
    /** 
     *  Streams are data changes captured (BEGIN, COMMIT, INSERT, UPDATE, DELETE, etc.) 
     *  via Slot Replication API and decoded. 
     */

    private PGReplicationStream repStream;
    private Long lastReceiveLSN;
    private Decode decode;
    private ConnectionManager connectionManager;

    public static final String MIME_TYPE_OUTPUT_DEFAULT = "application/json";

    public Stream(String pub, String slt, ConnectionManager connectionManager) throws SQLException {
        this(pub, slt, null, connectionManager);
    }

    public Stream(String pub, String slt, Long lsn, ConnectionManager connectionManager) throws SQLException {
        this.connectionManager = connectionManager;
        PGConnection pgcon = this.connectionManager.getReplicationConnection().unwrap(PGConnection.class);

        if (lsn == null) {
            this.repStream = pgcon.getReplicationAPI().replicationStream().logical().withSlotName(slt).withSlotOption("proto_version", "1").withSlotOption("publication_names", pub)
                    .withStatusInterval(1, TimeUnit.SECONDS).start();

        } else {
            // Reading from LSN start position
            LogSequenceNumber startLSN = LogSequenceNumber.valueOf(lsn);

            this.repStream = pgcon.getReplicationAPI().replicationStream().logical().withSlotName(slt).withSlotOption("proto_version", "1").withSlotOption("publication_names", pub)
                    .withStatusInterval(1, TimeUnit.SECONDS).withStartPosition(startLSN).start();
        }
        
        /**
         * More details about pgoutput options in PostgreSQL project,
         * source file postgres/src/backend/replication/pgoutput/pgoutput.c
         */
    }

    public Event readStream(boolean isSimpleEvent, boolean withBeginCommit, String outputFormat)
            throws SQLException, InterruptedException, ParseException, UnsupportedEncodingException, JsonProcessingException {

        LinkedList<String> messages = new LinkedList<String>();

        if (this.decode == null) {
            // First read
            this.decode = new Decode();
            decode.loadDataTypes(this.connectionManager);
        }

        while (true) {
            ByteBuffer buffer = this.repStream.readPending();

            if (buffer == null) {
                break;
            }

            HashMap<String, Object> message = null;

            if (isSimpleEvent) {
                message = this.decode.decodeLogicalReplicationMessageSimple(buffer, withBeginCommit);
            } else {
                message = this.decode.decodeLogicalReplicationMessage(buffer, withBeginCommit);
            }

            if (!message.isEmpty()) { // Skip empty messages
                messages.addLast(this.convertMessage(message, outputFormat.trim().toLowerCase()));
            }

            // Replication feedback
            this.repStream.setAppliedLSN(this.repStream.getLastReceiveLSN());
            this.repStream.setFlushedLSN(this.repStream.getLastReceiveLSN());
        }

        this.lastReceiveLSN = this.repStream.getLastReceiveLSN().asLong();

        return new Event(messages, this.lastReceiveLSN, isSimpleEvent, withBeginCommit, false);
    }

    public String convertMessage(HashMap<String, Object> message, String outputFormat) throws JsonProcessingException {

        switch (outputFormat) {
        case MIME_TYPE_OUTPUT_DEFAULT:

            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(message);

        default:

            throw new IllegalArgumentException("Invalid output format!");
        }
    }

    public Long getLastReceiveLSN() {
        return this.lastReceiveLSN;
    }
}