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

package org.apache.nifi.sql;

import org.apache.nifi.logging.ComponentLog;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class TestCalciteDatabase {

    @Test
    public void testSimpleQuery() throws SQLException, IOException {
        try (final CalciteDatabase database = createNameNumberDatabase();
             final PreparedStatement stmt = database.getConnection().prepareStatement("SELECT * FROM CANNED_DATA WHERE number > 100");
             final ResultSet resultSet = stmt.executeQuery()) {

            assertTrue(resultSet.next());

            assertEquals("Mark", resultSet.getString("name"));
            assertEquals("Mark", resultSet.getString(1));
            assertEquals(192, resultSet.getInt(2));
            assertEquals(192, resultSet.getInt("number"));

            assertTrue(resultSet.next());
            assertEquals("Jane Doe", resultSet.getString("name"));
            assertEquals("Jane Doe", resultSet.getString(1));
            assertEquals(2910, resultSet.getInt(2));
            assertEquals(2910, resultSet.getInt("number"));

            assertFalse(resultSet.next());
        }
    }

    @Test
    public void testAddFunction() throws SQLException, NoSuchMethodException, IOException {
        try (final CalciteDatabase database = createNameNumberDatabase()) {
            database.addUserDefinedFunction("UPS", ToUpperCase.class, "invoke");
            database.addUserDefinedFunction("UPS_METHOD", ToUpperCase.class.getDeclaredMethod("invoke", String.class));

            final String[] queries = new String[]{
                "SELECT UPS(name) AS UP FROM CANNED_DATA WHERE name NOT LIKE '% %'",
                "SELECT UPS_METHOD(name) AS UP FROM CANNED_DATA WHERE name NOT LIKE '% %'"
            };

            for (final String query : queries) {
                try (final PreparedStatement stmt = database.getConnection().prepareStatement(query);
                     final ResultSet resultSet = stmt.executeQuery()) {

                    assertTrue(resultSet.next());
                    assertEquals("MARK", resultSet.getString(1));
                    assertEquals("MARK", resultSet.getString("UP"));

                    assertTrue(resultSet.next());
                    assertEquals("OTHER", resultSet.getString(1));
                    assertEquals("OTHER", resultSet.getString("UP"));

                    assertFalse(resultSet.next());
                }
            }
        }
    }

    @Test
    public void testWithTimestamp() throws SQLException, IOException {
        final String query = "SELECT * FROM CANNED_DATA";

        try (final CalciteDatabase database = createNameTimestampDatabase();
                     final PreparedStatement stmt = database.getConnection().prepareStatement(query);
                     final ResultSet resultSet = stmt.executeQuery()) {

            assertTrue(resultSet.next());

            // We should get the same result whether we call getTimestamp() or getObject(). We should also get back the same original Long value.
            final Timestamp timestamp = resultSet.getTimestamp(2);
            assertEquals(timestamp, resultSet.getObject(2));
            assertEquals(1704056400000L, timestamp.getTime());

            assertFalse(resultSet.next());
        }
    }

    public static class ToUpperCase {
        public String invoke(final String value) {
            return value.toUpperCase();
        }
    }

    private CalciteDatabase createNameNumberDatabase() throws SQLException {
        final CalciteDatabase database = new CalciteDatabase();

        final NiFiTableSchema tableSchema = new NiFiTableSchema(List.of(
            new ColumnSchema("name", String.class, false),
            new ColumnSchema("number", int.class, false)
        ));

        final List<Object[]> rows = List.of(
            new Object[] {"Mark", 192},
            new Object[] {"John Doe", 41},
            new Object[] {"Jane Doe", 2910},
            new Object[] {"Other", -42}
        );
        final ListDataSource arrayListDataSource = new ListDataSource(tableSchema, rows);

        final NiFiTable table = new NiFiTable("CANNED_DATA", arrayListDataSource, mock(ComponentLog.class));
        database.addTable(table);

        return database;
    }

    private CalciteDatabase createNameTimestampDatabase() throws SQLException {
        final CalciteDatabase database = new CalciteDatabase();

        final NiFiTableSchema tableSchema = new NiFiTableSchema(List.of(
            new ColumnSchema("name", String.class, false),
            new ColumnSchema("dob", Timestamp.class, false)
        ));

        final List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[] {"Mark", new Timestamp(1704056400000L)});

        final ListDataSource arrayListDataSource = new ListDataSource(tableSchema, rows);

        final NiFiTable table = new NiFiTable("CANNED_DATA", arrayListDataSource, mock(ComponentLog.class));
        database.addTable(table);

        return database;
    }


    private static class ListDataSource implements ResettableDataSource {
        private final NiFiTableSchema schema;
        private final List<Object[]> rows;

        public ListDataSource(final NiFiTableSchema schema, final List<Object[]> rows) {
            this.schema = schema;
            this.rows = rows;
        }

        @Override
        public NiFiTableSchema getSchema() {
            return schema;
        }

        @Override
        public RowStream reset() {
            return new IterableRowStream<>(rows, row -> row);
        }
    }
}
