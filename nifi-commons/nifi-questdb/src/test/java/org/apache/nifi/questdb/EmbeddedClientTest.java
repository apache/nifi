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
package org.apache.nifi.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.questdb.QuestDbTestUtil.CREATE_EVENT2_TABLE;
import static org.apache.nifi.questdb.QuestDbTestUtil.CREATE_EVENT_TABLE;
import static org.apache.nifi.questdb.QuestDbTestUtil.EVENT2_TABLE_NAME;
import static org.apache.nifi.questdb.QuestDbTestUtil.EVENT_TABLE_INSERT_MAPPING;
import static org.apache.nifi.questdb.QuestDbTestUtil.EVENT_TABLE_INSERT_MAPPING_DIFFERENT_ORDER;
import static org.apache.nifi.questdb.QuestDbTestUtil.EVENT_TABLE_NAME;
import static org.apache.nifi.questdb.QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING_DIFFERENT_ORDER;
import static org.apache.nifi.questdb.QuestDbTestUtil.SELECT_QUERY;
import static org.apache.nifi.questdb.QuestDbTestUtil.SELECT_QUERY_2;
import static org.apache.nifi.questdb.QuestDbTestUtil.TEST_DB_PATH;

public class EmbeddedClientTest extends EmbeddedQuestDbTest {

    @Test
    public void testCreateInsertMappingWithoutFields() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> InsertMappingBuilder.of(Event.class).timestampAt(0).build());
    }

    @Test
    public void testCreateQueryMappingWithoutFields() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> RequestMappingBuilder.of(Event::new).build());
    }

    @Test
    public void testCreateMappingWithoutTimestamp() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> InsertMappingBuilder.of(Event.class).addField(String.class, Event::getSubject).build());
    }

    @Test
    public void testCreateMappingWithTimestampOutsideRange() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> InsertMappingBuilder.of(Event.class).addField(String.class, Event::getSubject).timestampAt(99).build());
    }

    @Test
    public void testInsertAndQuery() throws DatabaseException {
        final List<Event> testEvents = QuestDbTestUtil.getTestData();
        final Client client = getTestSubject();
        client.execute(CREATE_EVENT_TABLE);

        client.insert(EVENT_TABLE_NAME, InsertRowDataSource.forMapping(EVENT_TABLE_INSERT_MAPPING, testEvents));
        final Iterable<Event> result = client.query(SELECT_QUERY, QueryResultProcessor.forMapping(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));

        assertQueryResultMatchesWithInserts(result, testEvents);
    }

    @Test
    public void testInsertAndQueryWhenFieldOrderIsDifferent() throws DatabaseException {
        final List<Event> testEvents = QuestDbTestUtil.getTestData();
        final Client client = getTestSubject();
        client.execute(CREATE_EVENT2_TABLE);

        client.insert(EVENT2_TABLE_NAME, InsertRowDataSource.forMapping(EVENT_TABLE_INSERT_MAPPING_DIFFERENT_ORDER, testEvents));
        final Iterable<Event> result = client.query(SELECT_QUERY_2, QueryResultProcessor.forMapping(EVENT_TABLE_REQUEST_MAPPING_DIFFERENT_ORDER));

        assertQueryResultMatchesWithInserts(result, testEvents);
    }

    @Test
    public void testCannotExecuteOperationAfterDisconnected() throws DatabaseException {
        final Client client = getTestSubject();
        client.execute(CREATE_EVENT_TABLE);
        client.disconnect();
        Assertions.assertThrows(DatabaseException.class, () -> client.query(SELECT_QUERY, QueryResultProcessor.forMapping(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING)));
        Assertions.assertThrows(DatabaseException.class, () -> client.execute(SELECT_QUERY));
        Assertions.assertThrows(DatabaseException.class, () -> client.insert(EVENT_TABLE_NAME, InsertRowDataSource.forMapping(EVENT_TABLE_INSERT_MAPPING, QuestDbTestUtil.getTestData())));
    }

    private Client getTestSubject() {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(TEST_DB_PATH.toURI().getRawPath());
        final CairoEngine engine = new CairoEngine(configuration);
        return new EmbeddedClient(() -> engine);
    }

    private void assertQueryResultMatchesWithInserts(final Iterable<Event> result, final List<Event> testEvents) {
        assertNumberOfEntities(3, result);
        final Iterator<Event> iterator = result.iterator();
        Assertions.assertEquals(testEvents.get(0), iterator.next());
        Assertions.assertEquals(testEvents.get(1), iterator.next());
        Assertions.assertEquals(testEvents.get(2), iterator.next());
    }

    private void assertNumberOfEntities(final int expectedNumber, final Iterable<Event> iterable) {
        final AtomicInteger counted = new AtomicInteger(0);
        iterable.forEach(e -> counted.incrementAndGet());
        Assertions.assertEquals(expectedNumber, counted.get());
    }
}
