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

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

/*
 * Snapshots are copies of published table data.
 */
public class Snapshot {

    private final String publication;
    private final ConnectionManager connectionManager;

    public static final String MIME_TYPE_OUTPUT_DEFAULT = "application/json";

    public Snapshot(String pub, ConnectionManager connectionManager) {
        this.publication = Objects.requireNonNull(pub);
        this.connectionManager = Objects.requireNonNull(connectionManager);
    }

    public List<String> getPublicationTables() throws SQLException {
        try (PreparedStatement stmt = connectionManager.getSQLConnection().prepareStatement("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ?")) {
            stmt.setString(1, this.publication);
            try (ResultSet rs = stmt.executeQuery()) {
                List<String> pubTables = new ArrayList<>();
                while (rs.next()) {
                    pubTables.add(rs.getString(1) + "." + rs.getString(2));
                }
                return pubTables;
            }
        }
    }

    // WARNING : this method loads the entire table in memory!
    private List<String> getInitialSnapshotTableJSON(String tableName) throws SQLException, IOException {
        PGConnection pgcon = connectionManager.getSQLConnection().unwrap(PGConnection.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        CopyManager manager = pgcon.getCopyAPI();
        manager.copyOut("COPY (SELECT REGEXP_REPLACE(ROW_TO_JSON(t)::TEXT, '\\\\\\\\', '\\\\', 'g') FROM (SELECT * FROM " + tableName + ") t) TO STDOUT ", out);
        return new ArrayList<>(Arrays.asList(out.toString(StandardCharsets.UTF_8.name()).split("\n")));
    }

    public Event getInitialSnapshot(String outputFormat) throws SQLException, IOException {
        List<String> snapshot = new LinkedList<>();

        List<String> pubTables = this.getPublicationTables();

        switch (outputFormat) {
            case MIME_TYPE_OUTPUT_DEFAULT:

                for (String table : pubTables) {
                    List<String> lines = getInitialSnapshotTableJSON(table);

                    snapshot.add("{\"snapshot\":{\"relationName\":\"" + table + "\",\"tupleData\":" + lines.toString().replace("\\\\\"", "\\\"") + "}}");
                }

                return new Event(snapshot, null, true, false, true);

            default:
                throw new IllegalArgumentException("Invalid output format " + outputFormat);
        }
    }
}