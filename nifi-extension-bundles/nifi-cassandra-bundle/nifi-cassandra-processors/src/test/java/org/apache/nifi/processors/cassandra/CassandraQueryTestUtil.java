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

import org.apache.nifi.cassandra.exception.CassandraException;
import org.apache.nifi.cassandra.exception.CassandraExceptionCategory;
import org.apache.nifi.cassandra.models.CassandraColumnDefinition;
import org.apache.nifi.cassandra.models.CassandraQueryResult;
import org.apache.nifi.cassandra.models.CassandraRow;
import org.apache.nifi.cassandra.models.CassandraType;
import org.apache.nifi.cassandra.models.CassandraTypeName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility methods for Cassandra processors' unit tests
 */
public final class CassandraQueryTestUtil {

    private static final List<CassandraColumnDefinition> COLUMNS = List.of(
            new CassandraColumnDefinition("user_id", CassandraType.primitive(CassandraTypeName.TEXT)),
            new CassandraColumnDefinition("first_name", CassandraType.primitive(CassandraTypeName.TEXT)),
            new CassandraColumnDefinition("last_name", CassandraType.primitive(CassandraTypeName.TEXT)),
            new CassandraColumnDefinition("emails", CassandraType.set(CassandraType.primitive(CassandraTypeName.TEXT))),
            new CassandraColumnDefinition("top_places",
                    CassandraType.list(CassandraType.primitive(CassandraTypeName.TEXT))),
            new CassandraColumnDefinition("todo", CassandraType.map(CassandraType.primitive(CassandraTypeName.TEXT))),
            new CassandraColumnDefinition("registered", CassandraType.primitive(CassandraTypeName.BOOLEAN)),
            new CassandraColumnDefinition("scale", CassandraType.primitive(CassandraTypeName.FLOAT)),
            new CassandraColumnDefinition("metric", CassandraType.primitive(CassandraTypeName.DOUBLE)));

    public static CassandraQueryResult createMockQueryResult(final boolean twoPages) {
        final List<CassandraRow> firstPage = List.of(createMockRow("user1"));
        final List<CassandraRow> secondPage = List.of(createMockRow("user2"));
        return twoPages
                ? new TestCassandraQueryResult(COLUMNS, firstPage, true,
                        new TestCassandraQueryResult(COLUMNS, secondPage, false, null))
                : new TestCassandraQueryResult(COLUMNS, List.of(createMockRow("user1"), createMockRow("user2")), false,
                        null);
    }

    public static CassandraRow createMockRow(final String userId) {
        final Map<Integer, Object> values = new HashMap<>();

        if ("user1".equals(userId)) {
            values.put(0, "user1");
            values.put(1, "Joe");
            values.put(2, "Smith");
            values.put(3, Set.of("jsmith@notareal.com"));
            values.put(4, List.of("New York, NY", "Santa Clara, CA"));
            values.put(5, Map.of("2016-01-03 05:00:00+0000", "Set my alarm \"for\" a month from now"));
            values.put(6, false);
            values.put(7, 1.0f);
            values.put(8, 2.0d);
        } else {
            values.put(0, "user2");
            values.put(1, "Mary");
            values.put(2, "Jones");
            values.put(3, Set.of("mjones@notareal.com"));
            values.put(4, List.of("Orlando, FL"));
            values.put(5, Map.of("2016-02-03 05:00:00+0000", "Get milk and bread"));
            values.put(6, true);
            values.put(7, 3.0f);
            values.put(8, 4.0d);
        }

        return new TestCassandraRow(values, COLUMNS);
    }

    private record TestCassandraQueryResult(
            List<CassandraColumnDefinition> columnDefinitions,
            List<CassandraRow> currentPage,
            boolean hasMorePages,
            CassandraQueryResult nextPage) implements CassandraQueryResult {

        @Override
        public List<CassandraColumnDefinition> getColumnDefinitions() {
            return columnDefinitions;
        }

        @Override
        public Iterable<CassandraRow> getCurrentPage() {
            return currentPage;
        }

        @Override
        public CassandraQueryResult fetchNextPage() throws CassandraException {
            if (!hasMorePages) {
                throw new CassandraException("No more pages",
                        CassandraExceptionCategory.FAILURE, null);
            }
            return nextPage;
        }
    }

    private record TestCassandraRow(Map<Integer, Object> values, List<CassandraColumnDefinition> columns)
            implements CassandraRow {
        @Override
        public boolean isNull(final int index) {
            return !values.containsKey(index) || values.get(index) == null;
        }

        @Override
        public Object getValue(final int index) {
            return values.get(index);
        }

        @Override
        public CassandraType getType(final int index) {
            return columns.get(index).type();
        }
    }
}

