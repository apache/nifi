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

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Utility methods for Cassandra processors' unit tests
 */
public class CassandraQueryTestUtil {
    public static ResultSet createMockResultSet() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        when(columnDefinitions.size()).thenReturn(9);
        when(columnDefinitions.getName(anyInt())).thenAnswer(new Answer<String>() {

            List<String> colNames = Arrays.asList(
                    "user_id", "first_name", "last_name", "emails", "top_places", "todo", "registered", "scale", "metric");

            @Override
            public String answer(InvocationOnMock invocationOnMock) throws Throwable {
                return colNames.get((Integer) invocationOnMock.getArguments()[0]);

            }
        });

        when(columnDefinitions.getTable(0)).thenReturn("users");

        when(columnDefinitions.getType(anyInt())).thenAnswer(new Answer<DataType>() {

            List<DataType> dataTypes = Arrays.asList(
                    DataType.text(), DataType.text(), DataType.text(), DataType.set(DataType.text()),
                    DataType.list(DataType.text()), DataType.map(DataType.timestamp(), DataType.text()), DataType.cboolean(),
                    DataType.cfloat(), DataType.cdouble()
            );

            @Override
            public DataType answer(InvocationOnMock invocationOnMock) throws Throwable {
                return dataTypes.get((Integer) invocationOnMock.getArguments()[0]);

            }
        });

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        final Date aMonthPrior = dateFormat.parse("2016-01-03 05:00:00+0000");
        final Date testDate = dateFormat.parse("2016-02-03 05:00:00+0000");
        List<Row> rows = Arrays.asList(
                createRow("user1", "Joe", "Smith", Sets.newHashSet("jsmith@notareal.com"),
                        Arrays.asList("New York, NY", "Santa Clara, CA"),
                        new HashMap<Date, String>() {{
                            put(aMonthPrior, "Set my alarm \"for\" a month from now");
                        }}, false, 1.0f, 2.0),
                createRow("user2", "Mary", "Jones", Sets.newHashSet("mjones@notareal.com"),
                        Collections.singletonList("Orlando, FL"),
                        new HashMap<Date, String>() {{
                            put(testDate, "Get milk and bread");
                        }}, true, 3.0f, 4.0)
        );

        when(resultSet.iterator()).thenReturn(rows.iterator());
        when(resultSet.all()).thenReturn(rows);
        when(resultSet.getAvailableWithoutFetching()).thenReturn(rows.size());
        when(resultSet.isFullyFetched()).thenReturn(false).thenReturn(true);
        when(resultSet.getColumnDefinitions()).thenReturn(columnDefinitions);
        return resultSet;
    }

    public static ResultSet createMockResultSetOneColumn() throws Exception {
        ResultSet resultSet = mock(ResultSet.class);
        ColumnDefinitions columnDefinitions = mock(ColumnDefinitions.class);
        when(columnDefinitions.size()).thenReturn(1);
        when(columnDefinitions.getName(anyInt())).thenAnswer(new Answer<String>() {
            List<String> colNames = Arrays.asList("user_id");
            @Override
            public String answer(InvocationOnMock invocationOnMock) throws Throwable {
                return colNames.get((Integer) invocationOnMock.getArguments()[0]);

            }
        });

        when(columnDefinitions.getTable(0)).thenReturn("users");

        when(columnDefinitions.getType(anyInt())).thenAnswer(new Answer<DataType>() {
            List<DataType> dataTypes = Arrays.asList(DataType.text());
            @Override
            public DataType answer(InvocationOnMock invocationOnMock) throws Throwable {
                return dataTypes.get((Integer) invocationOnMock.getArguments()[0]);

            }
        });

        List<Row> rows = Arrays.asList(
                createRow("user1"),
                createRow("user2")
        );

        when(resultSet.iterator()).thenReturn(rows.iterator());
        when(resultSet.all()).thenReturn(rows);
        when(resultSet.getAvailableWithoutFetching()).thenReturn(rows.size());
        when(resultSet.isFullyFetched()).thenReturn(false).thenReturn(true);
        when(resultSet.getColumnDefinitions()).thenReturn(columnDefinitions);
        return resultSet;
    }

    public static Row createRow(String user_id, String first_name, String last_name, Set<String> emails,
                                List<String> top_places, Map<Date, String> todo, boolean registered,
                                float scale, double metric) {
        Row row = mock(Row.class);
        when(row.getString(0)).thenReturn(user_id);
        when(row.getString(1)).thenReturn(first_name);
        when(row.getString(2)).thenReturn(last_name);
        when(row.getSet(eq(3), any(TypeToken.class))).thenReturn(emails);
        when(row.getList(eq(4), any(TypeToken.class))).thenReturn(top_places);
        when(row.getMap(eq(5), any(TypeToken.class), any(TypeToken.class))).thenReturn(todo);
        when(row.getBool(6)).thenReturn(registered);
        when(row.getFloat(7)).thenReturn(scale);
        when(row.getDouble(8)).thenReturn(metric);

        return row;
    }

    public static Row createRow(String user_id) {
        Row row = mock(Row.class);
        when(row.getString(0)).thenReturn(user_id);
        return row;
    }
}
