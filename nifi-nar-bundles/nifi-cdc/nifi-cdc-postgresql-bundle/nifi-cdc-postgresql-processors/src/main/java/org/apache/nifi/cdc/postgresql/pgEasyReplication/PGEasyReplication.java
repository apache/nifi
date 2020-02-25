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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import org.postgresql.PGConnection;

import org.apache.nifi.cdc.postgresql.pgEasyReplication.Event;
import org.apache.nifi.cdc.postgresql.pgEasyReplication.Snapshot;

public class PGEasyReplication {

    private String publication;
    private String slot;
    private boolean slotDropIfExists;
    private Stream stream;
    private ConnectionManager connectionManager;

    public PGEasyReplication(String pub, String slt, boolean sltDropIfExists, ConnectionManager connectionManager) {
        this.publication = pub;
        this.slot = slt;
        this.slotDropIfExists = sltDropIfExists;
        this.connectionManager = connectionManager;
    }

    public void initializeLogicalReplication() {
        try {
            PreparedStatement stmt = this.connectionManager.getSQLConnection().prepareStatement("select 1 from pg_catalog.pg_replication_slots WHERE slot_name = ?");

            stmt.setString(1, this.slot);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) { // If slot exists
                if (this.slotDropIfExists) {
                    this.dropReplicationSlot();
                    this.createReplicationSlot();
                }
            } else {
                this.createReplicationSlot();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void createReplicationSlot() {
        try {
            PGConnection pgcon = this.connectionManager.getReplicationConnection().unwrap(PGConnection.class);
            // More details about pgoutput options: https://github.com/postgres/postgres/blob/master/src/backend/replication/pgoutput/pgoutput.c
            //PostgreSQL at GitHub: https://github.com/postgres
            //Source file: postgres/src/backend/replication/pgoutput/pgoutput.c
            pgcon.getReplicationAPI()
            .createReplicationSlot()
            .logical()
            .withSlotName(this.slot)
            .withOutputPlugin("pgoutput")
            .make();

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void dropReplicationSlot() {
        try {
            PGConnection pgcon = this.connectionManager.getReplicationConnection().unwrap(PGConnection.class);
            pgcon.getReplicationAPI().dropReplicationSlot(this.slot);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Event getSnapshot() {
        Event event = null;

        try {
            Snapshot snapshot = new Snapshot(this.publication, this.connectionManager);
            event = snapshot.getInitialSnapshot();

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return event;
    }

    public Event readEvent() {
        return this.readEvent(true, true, null);
    }

    public Event readEvent(boolean isSimpleEvent) {
        return this.readEvent(isSimpleEvent, true, null);
    }

    public Event readEvent(boolean isSimpleEvent, boolean withBeginCommit) {
        return this.readEvent(isSimpleEvent, withBeginCommit, null);
    }

    public Event readEvent(boolean isSimpleEvent, boolean withBeginCommit, Long startLSN) {
        Event event = null;

        try {
            if (this.stream == null) { // First read
                this.stream = new Stream(this.publication, this.slot, startLSN, this.connectionManager);
            }

            event = this.stream.readStream(isSimpleEvent, withBeginCommit);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }

        return event;
    }
}
