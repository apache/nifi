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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.postgresql.PGConnection;

/**
 * Slot is the helper class for PostgreSQL Replication slot, which is necessary
 * for start replication protocol. The Slot name is used to check if the
 * replication slot already exists in PostgreSQL Cluster. If so, it can continue
 * using its, or drop and create a new one.
 */
public class Slot {

    private String name;

    public Slot(String name, boolean dropSlotIfExists, Connection replicationConn, Connection queryConn)
            throws SQLException {

        this.name = name;
        this.create(dropSlotIfExists, replicationConn, queryConn);
    }

    /**
     * Creates the PostgreSQL Replication Slot.
     *
     * @param dropSlotIfExists
     *                         Indicates whether the replication slot should be
     *                         recreated if already exists.
     * @param replicationConn
     *                         Replication Connection to create and drop replication
     *                         slot.
     * @param queryConn
     *                         Query Connection to check replication slot existence.
     * @throws SQLException
     *                      if fails to access PostgreSQL Cluster
     */
    public void create(boolean dropSlotIfExists, Connection replicationConn, Connection queryConn) throws SQLException {
        try {
            if (exists(queryConn)) {
                if (dropSlotIfExists)
                    drop(replicationConn);
                else
                    return;
            }

            PGConnection pgcon = replicationConn.unwrap(PGConnection.class);
            pgcon.getReplicationAPI().createReplicationSlot().logical().withSlotName(this.name)
                    .withOutputPlugin("pgoutput").make();

        } catch (SQLException e) {
            throw new SQLException("Failed to create replication slot. " + e, e);
        }
    }

    /**
     * Checks the PostgreSQL Replicaton Slot existence.
     *
     * @param queryConn
     *                  Query Connection to check replication slot existence in
     *                  pg_catalog.pg_replication_slots system view.
     * @return boolean
     * @throws SQLException
     *                      if fails to access PostgreSQL Cluster
     */
    public boolean exists(Connection queryConn) throws SQLException {
        try {
            PreparedStatement stmt = queryConn
                    .prepareStatement("select 1 from pg_catalog.pg_replication_slots WHERE slot_name = ?");

            stmt.setString(1, this.name);
            ResultSet rs = stmt.executeQuery();

            if (rs.next()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            throw new SQLException("Failed to check replication slot existence. " + e, e);
        }
    }

    /**
     * Removes the Replication Slot from PostgreSQL Cluster.
     *
     * @param replicationConn
     *                        Replication Connection to drop replication slot.
     * @throws SQLException
     *                      if fails to access PostgreSQL Cluster
     */
    public void drop(Connection replicationConn) throws SQLException {
        try {
            PGConnection pgcon = replicationConn.unwrap(PGConnection.class);
            pgcon.getReplicationAPI().dropReplicationSlot(this.name);
        } catch (SQLException e) {
            throw new SQLException("Failed to drop replication slot. " + e, e);
        }
    }

    /**
     * Returns the Replication Slot name.
     *
     * @return String
     */
    public String getName() {
        return this.name;
    }
}