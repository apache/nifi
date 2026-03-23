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
package org.apache.nifi.processors.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Utility methods for Cassandra processors' unit tests
 */
public class CassandraQueryTestUtil {

    static final Date TEST_DATE;
    static {
        Calendar c = GregorianCalendar.getInstance(TimeZone.getTimeZone("PST"));
        c.set(2020, Calendar.JANUARY, 1, 10, 10, 10);
        c.set(Calendar.MILLISECOND, 10);
        TEST_DATE = c.getTime();
    }

    public static AsyncResultSet createMockAsyncResultSet(boolean twoPages) throws Exception {
        AsyncResultSet firstPage = mock(AsyncResultSet.class);
        AsyncResultSet secondPage = mock(AsyncResultSet.class);

        Row row1 = createMockRow("user1");
        Row row2 = createMockRow("user2");

        if (twoPages) {
            when(firstPage.currentPage()).thenReturn(List.of(row1));
            when(firstPage.hasMorePages()).thenReturn(true);
            when(firstPage.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(secondPage));

            when(secondPage.currentPage()).thenReturn(List.of(row2));
            when(secondPage.hasMorePages()).thenReturn(false);
            when(secondPage.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(secondPage));
        } else {
            when(firstPage.currentPage()).thenReturn(List.of(row1, row2));
            when(firstPage.hasMorePages()).thenReturn(false);
            when(firstPage.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(firstPage));
        }
        return firstPage;
    }

    private static void mockValue(Row row, int index, Object value) {
        if (value instanceof String) {
            when(row.getString(index)).thenReturn((String) value);
        } else if (value instanceof Set) {
            when(row.getSet(eq(index), eq(String.class))).thenReturn((Set<String>) value);
        } else if (value instanceof List) {
            when(row.getList(eq(index), eq(String.class))).thenReturn((List<String>) value);
        } else if (value instanceof Map) {
            when(row.getMap(eq(index), eq(String.class), eq(String.class))).thenReturn((Map<String, String>) value);
        } else if (value instanceof Boolean) {
            when(row.getBoolean(index)).thenReturn((Boolean) value);
        } else if (value instanceof Float) {
            when(row.getFloat(index)).thenReturn((Float) value);
        } else if (value instanceof Double) {
            when(row.getDouble(index)).thenReturn((Double) value);
        }
    }

    private static ColumnDefinition mockColumn(String name, DataType type) {
        ColumnDefinition cd = mock(ColumnDefinition.class);
        when(cd.getName()).thenReturn(CqlIdentifier.fromInternal(name));
        when(cd.getType()).thenReturn(type);
        return cd;
    }

    public static Row createMockRow(String userId) {
        Row row = mock(Row.class);

        ColumnDefinitions colDefs = mock(ColumnDefinitions.class);

        List<ColumnDefinition> columns = List.of(
                mockColumn("user_id", DataTypes.TEXT),
                mockColumn("first_name", DataTypes.TEXT),
                mockColumn("last_name", DataTypes.TEXT),
                mockColumn("emails", DataTypes.setOf(DataTypes.TEXT)),
                mockColumn("top_places", DataTypes.listOf(DataTypes.TEXT)),
                mockColumn("todo", DataTypes.mapOf(DataTypes.TEXT, DataTypes.TEXT)),
                mockColumn("registered", DataTypes.BOOLEAN),
                mockColumn("scale", DataTypes.FLOAT),
                mockColumn("metric", DataTypes.DOUBLE)
        );

        when(colDefs.size()).thenReturn(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            when(colDefs.get(i)).thenReturn(columns.get(i));
        }
        when(colDefs.iterator()).thenAnswer(inv -> columns.iterator());
        when(row.getColumnDefinitions()).thenReturn(colDefs);

        Map<String, String> todoMap = new HashMap<>();

        if (userId.equals("user1")) {
            todoMap.put("2016-01-03 05:00:00+0000", "Set my alarm \"for\" a month from now");

            mockValue(row, 0, "user1");
            mockValue(row, 1, "Joe");
            mockValue(row, 2, "Smith");
            mockValue(row, 3, Set.of("jsmith@notareal.com"));
            mockValue(row, 4, List.of("New York, NY", "Santa Clara, CA"));
            mockValue(row, 5, todoMap);
            mockValue(row, 6, false);
            mockValue(row, 7, 1.0f);
            mockValue(row, 8, 2.0);
        } else {
            todoMap.put("2016-02-03 05:00:00+0000", "Get milk and bread");

            mockValue(row, 0, "user2");
            mockValue(row, 1, "Mary");
            mockValue(row, 2, "Jones");
            mockValue(row, 3, Set.of("mjones@notareal.com"));
            mockValue(row, 4, List.of("Orlando, FL"));
            mockValue(row, 5, todoMap);
            mockValue(row, 6, true);
            mockValue(row, 7, 3.0f);
            mockValue(row, 8, 4.0);
        }
        when(row.isNull(anyInt())).thenReturn(false);
        return row;
    }

    public static AsyncResultSet createMockAsyncResultSetOneColumn() {
        AsyncResultSet resultSet = mock(AsyncResultSet.class);

        ColumnDefinition columnDef = mock(ColumnDefinition.class);
        CqlIdentifier columnName = mock(CqlIdentifier.class);
        when(columnName.asInternal()).thenReturn("user_id");
        when(columnDef.getName()).thenReturn(columnName);
        when(columnDef.getType()).thenReturn(DataTypes.TEXT);

        ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        when(columnDefinitions.size()).thenReturn(1);
        when(columnDefinitions.get(0)).thenReturn(columnDef);

        when(resultSet.getColumnDefinitions()).thenReturn(columnDefinitions);

        Row row1 = mock(Row.class);
        when(row1.getColumnDefinitions()).thenReturn(columnDefinitions);
        when(row1.getString("user_id")).thenReturn("user1");

        Row row2 = mock(Row.class);
        when(row2.getColumnDefinitions()).thenReturn(columnDefinitions);
        when(row2.getString("user_id")).thenReturn("user2");

        List<Row> rows = Arrays.asList(row1, row2);

        when(resultSet.currentPage()).thenReturn(rows);
        when(resultSet.hasMorePages()).thenReturn(false);
        when(resultSet.fetchNextPage()).thenReturn(CompletableFuture.completedFuture(resultSet));

        return resultSet;
    }

    public static Iterable<Row> createMockDateRows() {
        ColumnDefinition columnDef = mock(ColumnDefinition.class);
        CqlIdentifier columnName = mock(CqlIdentifier.class);
        when(columnName.asInternal()).thenReturn("date");
        when(columnDef.getName()).thenReturn(columnName);
        when(columnDef.getType()).thenReturn(DataTypes.TIMESTAMP);

        ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        when(columnDefinitions.size()).thenReturn(1);
        when(columnDefinitions.get(0)).thenReturn(columnDef);

        Row row = mock(Row.class);
        when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
        when(row.isNull(0)).thenReturn(false);
        when(row.getInstant(0)).thenReturn(TEST_DATE.toInstant());
        return List.of(row);
    }
}
