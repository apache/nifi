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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

public class Snapshot {

    private String publication;
    private ConnectionManager connectionManager;

    public Snapshot(String pub, ConnectionManager connectionManager) {
        this.publication = pub;
        this.connectionManager = connectionManager;
    }

    public ArrayList<String> getPublicationTables() throws SQLException {
        PreparedStatement stmt = this.connectionManager.getSQLConnection().prepareStatement("SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = ?");

        stmt.setString(1, this.publication);
        ResultSet rs = stmt.executeQuery();

        ArrayList<String> pubTables = new ArrayList<String>();

        while (rs.next()) {
            pubTables.add(rs.getString(1) + "." + rs.getString(2));
        }

        rs.close();
        stmt.close();

        return pubTables;
    }

    public ArrayList<String> getInitialSnapshotTable(String tableName) throws SQLException, IOException {
        PGConnection pgcon = this.connectionManager.getSQLConnection().unwrap(PGConnection.class);
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        CopyManager manager = pgcon.getCopyAPI();
        manager.copyOut("COPY (SELECT REGEXP_REPLACE(ROW_TO_JSON(t)::TEXT, '\\\\\\\\', '\\\\', 'g') FROM (SELECT * FROM " + tableName + ") t) TO STDOUT ", out);

        return new ArrayList<String>(Arrays.asList(out.toString("UTF-8").split("\n")));
    }

    public Event getInitialSnapshot() throws SQLException, IOException {
        LinkedList<String> snapshot = new LinkedList<String>();

        ArrayList<String> pubTables = this.getPublicationTables();

        for (String table : pubTables) {
            ArrayList<String> lines = this.getInitialSnapshotTable(table);

            snapshot.add("{\"snaphost\":{\"" + table + "\":" + lines.toString().replace("\\\\\"", "\\\"") + "}}");
        }

        return new Event(snapshot, null, true, false, false);
    }
}
