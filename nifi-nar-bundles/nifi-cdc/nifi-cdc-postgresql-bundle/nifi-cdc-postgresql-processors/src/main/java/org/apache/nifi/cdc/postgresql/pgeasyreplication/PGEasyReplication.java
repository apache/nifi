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
import org.apache.nifi.cdc.CDCException;
import org.apache.nifi.logging.ComponentLog;
import org.postgresql.PGConnection;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Objects;

public class PGEasyReplication {

    private final String publication;
    private final String slot;
    private final ConnectionManager connectionManager;
    private final ComponentLog log;

    private Stream stream;

    public PGEasyReplication(String pub, String slt, ConnectionManager connectionManager, ComponentLog log) {
        this.publication = Objects.requireNonNull(pub);
        this.slot = Objects.requireNonNull(slt);
        this.connectionManager = Objects.requireNonNull(connectionManager);
        this.log = Objects.requireNonNull(log);
    }

    public void initializeLogicalReplication(boolean slotDropIfExists) {
        try (PreparedStatement stmt = this.connectionManager.getSQLConnection().prepareStatement("select 1 from pg_catalog.pg_replication_slots WHERE slot_name = ?")) {
            stmt.setString(1, slot);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    log.info("Logical replication slot {} found", slot);
                    // If slot exists
                    if (slotDropIfExists) {
                        this.dropReplicationSlot();
                        this.createReplicationSlot();
                    }
                } else {
                    this.createReplicationSlot();
                }
            }
        } catch (SQLException sqlEx) {
            throw new IllegalStateException("Failed to initialize the logical replication slot", sqlEx);
        }
    }

    private void createReplicationSlot() throws SQLException {
        log.info("Creating replication slot {}", slot);
        PGConnection pgcon = connectionManager.getReplicationConnection().unwrap(PGConnection.class);

        // More details about pgoutput options in PostgreSQL project: https://github.com/postgres, source file: postgres/src/backend/replication/pgoutput/pgoutput.c
        pgcon.getReplicationAPI()
                .createReplicationSlot()
                .logical()
                .withSlotName(slot)
                .withOutputPlugin("pgoutput")
                .make();
    }

    private void dropReplicationSlot() throws SQLException {
        log.info("Dropping replication slot {}", slot);
        PGConnection pgcon = connectionManager.getReplicationConnection().unwrap(PGConnection.class);
        pgcon.getReplicationAPI().dropReplicationSlot(slot);
    }

    public Event getSnapshot(String outputFormat) throws CDCException {
        try {
            Snapshot snapshot = new Snapshot(this.publication, this.connectionManager);
            return snapshot.getInitialSnapshot(outputFormat);
        } catch (SQLException | IOException ex) {
            throw new CDCException("Error while obtaining snapshot", ex);
        }
    }

    public Event readEvent(boolean isSimpleEvent, boolean withBeginCommit, String outputFormat) throws CDCException {
        return readEvent(isSimpleEvent, withBeginCommit, outputFormat, null);
    }

    public Event readEvent(boolean isSimpleEvent, boolean withBeginCommit, String outputFormat, Long startLSN) throws CDCException {
        try {
            if (stream == null) {
                // First read stream
                log.debug("Initialize replication stream");
                stream = new Stream(publication, slot, connectionManager, startLSN);
            }
            return stream.readStream(isSimpleEvent, withBeginCommit, outputFormat);
        } catch (SQLException | InterruptedException | ParseException | UnsupportedEncodingException | JsonProcessingException ex) {
            throw new CDCException("Error while reading CDC event", ex);
        }
    }

}