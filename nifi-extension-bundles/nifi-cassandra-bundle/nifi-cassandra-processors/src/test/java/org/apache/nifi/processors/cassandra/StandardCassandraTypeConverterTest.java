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

import org.apache.nifi.cassandra.models.CassandraRow;
import org.apache.nifi.processors.cassandra.converter.StandardCassandraTypeConverter;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the StandardCassandraTypeConverter
 */
class StandardCassandraTypeConverterTest {
    private final StandardCassandraTypeConverter typeConverter = new StandardCassandraTypeConverter();

    @Test
    void testGetCassandraObject() throws Exception {
        CassandraRow row = CassandraQueryTestUtil.createMockRow("user1");
        assertEquals("user1", typeConverter.getCassandraObject(row, 0));
        assertEquals("Joe", typeConverter.getCassandraObject(row, 1));
        assertEquals("Smith", typeConverter.getCassandraObject(row, 2));
        Set<String> emails = (Set<String>) typeConverter.getCassandraObject(row, 3);
        assertNotNull(emails);
        assertEquals(1, emails.size());
        assertTrue(emails.contains("jsmith@notareal.com"));
        List<String> topPlaces = (List<String>) typeConverter.getCassandraObject(row, 4);
        assertNotNull(topPlaces);
        assertEquals(2, topPlaces.size());
        assertEquals("New York, NY", topPlaces.get(0));
        Map<String, String> todoMap = (Map<String, String>) typeConverter.getCassandraObject(row, 5);
        assertNotNull(todoMap);
        assertEquals(1, todoMap.size());
        assertEquals("Set my alarm \"for\" a month from now", todoMap.get("2016-01-03 05:00:00+0000"));
        Boolean registered = (Boolean) typeConverter.getCassandraObject(row, 6);
        assertNotNull(registered);
        assertFalse(registered);
        assertEquals(1.0f, (Float) typeConverter.getCassandraObject(row, 7), 0.001);
        assertEquals(2.0, (Double) typeConverter.getCassandraObject(row, 8), 0.001);
    }
}

