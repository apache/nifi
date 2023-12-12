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
package org.apache.nifi.questdb.embedded;

import org.apache.nifi.questdb.Client;
import org.apache.nifi.questdb.DatabaseException;
import org.apache.nifi.questdb.mapping.RequestMapping;
import org.apache.nifi.questdb.util.Event;
import org.apache.nifi.questdb.util.QuestDbTestUtil;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.nifi.questdb.util.QuestDbTestUtil.CREATE_EVENT2_TABLE;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.CREATE_EVENT_TABLE;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.EVENT2_TABLE_NAME;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.EVENT_TABLE_NAME;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING_DIFFERENT_ORDER;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.SELECT_QUERY;
import static org.apache.nifi.questdb.util.QuestDbTestUtil.SELECT_QUERY_2;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EmbeddedClientTest extends ManagedQuestDbTest {

    @Test
    public void testInsertAndQuery() throws DatabaseException {
        final List<Event> testEvents = QuestDbTestUtil.getTestData();
        final Client client = getTestSubject();
        client.execute(CREATE_EVENT_TABLE);

        client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(testEvents));
        final Iterable<Event> result = client.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));

        assertQueryResultMatchesWithInserts(result, testEvents);
    }

    @Test
    public void testInsertAndQueryWhenFieldOrderIsDifferent() throws DatabaseException {
        final List<Event> testEvents = QuestDbTestUtil.getTestData();
        final Client client = getTestSubject();
        client.execute(CREATE_EVENT2_TABLE);

        client.insert(EVENT2_TABLE_NAME, QuestDbTestUtil.getEventTableDataSourceWithDifferentOrder(testEvents));
        final Iterable<Event> result = client.query(SELECT_QUERY_2, RequestMapping.getResultProcessor(EVENT_TABLE_REQUEST_MAPPING_DIFFERENT_ORDER));

        assertQueryResultMatchesWithInserts(result, testEvents);
    }

    @Test
    public void testCannotExecuteOperationAfterDisconnected() throws DatabaseException {
        final Client client = getTestSubject();
        client.execute(CREATE_EVENT_TABLE);
        client.disconnect();
        assertThrows(DatabaseException.class, () -> client.query(SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING)));
        assertThrows(DatabaseException.class, () -> client.execute(SELECT_QUERY));
        assertThrows(DatabaseException.class, () -> client.insert(EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(QuestDbTestUtil.getTestData())));
    }

    private Client getTestSubject() {
        return EmbeddedQuestDbTestUtil.getEmbeddedClient(engine);
    }

    private void assertQueryResultMatchesWithInserts(final Iterable<Event> result, final List<Event> testEvents) {
        assertNumberOfEntities(3, result);
        final Iterator<Event> iterator = result.iterator();
        assertEquals(testEvents.get(0), iterator.next());
        assertEquals(testEvents.get(1), iterator.next());
        assertEquals(testEvents.get(2), iterator.next());
    }

    private void assertNumberOfEntities(final int expectedNumber, final Iterable<Event> iterable) {
        final AtomicInteger counted = new AtomicInteger(0);
        iterable.forEach(e -> counted.incrementAndGet());
        assertEquals(expectedNumber, counted.get());
    }
}
