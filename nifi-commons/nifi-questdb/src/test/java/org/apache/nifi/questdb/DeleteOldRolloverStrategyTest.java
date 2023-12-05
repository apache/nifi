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

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.StreamSupport;

import static org.apache.nifi.questdb.QuestDbTestUtil.CREATE_EVENT_TABLE;
import static org.apache.nifi.questdb.QuestDbTestUtil.EVENT_TABLE_INSERT_MAPPING;
import static org.apache.nifi.questdb.QuestDbTestUtil.EVENT_TABLE_NAME;
import static org.apache.nifi.questdb.QuestDbTestUtil.SELECT_QUERY;
import static org.apache.nifi.questdb.QuestDbTestUtil.TEST_DB_PATH;

class DeleteOldRolloverStrategyTest extends EmbeddedQuestDbTest {

    @Test
    public void testCleaningUpOldData() throws DatabaseException {
        final Client client = getTestSubject();
        final DeleteOldRolloverStrategy testSubject = new DeleteOldRolloverStrategy(ZonedDateTime::now, 1);

        final List<Event> entries = new ArrayList<>();
        entries.add(new Event(Instant.now().minus(2, ChronoUnit.DAYS), "A", 1));
        entries.add(new Event(Instant.now(), "B", 2));

        client.execute(CREATE_EVENT_TABLE);
        client.insert(EVENT_TABLE_NAME, InsertRowDataSource.forMapping(EVENT_TABLE_INSERT_MAPPING, entries));
        final Iterable<Event> resultBeforeRollover = client.query(SELECT_QUERY, QueryResultProcessor.forMapping(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        Assertions.assertEquals(2, StreamSupport.stream(resultBeforeRollover.spliterator(), false).count());

        testSubject.rollOver(client, EVENT_TABLE_NAME);

        final Iterable<Event> resultAfterRollover = client.query(SELECT_QUERY, QueryResultProcessor.forMapping(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        Assertions.assertEquals(1, StreamSupport.stream(resultAfterRollover.spliterator(), false).count());

        // Multiple runs should not delete further data
        testSubject.rollOver(client, EVENT_TABLE_NAME);
        testSubject.rollOver(client, EVENT_TABLE_NAME);

        final Iterable<Event> resultAfterMultipleRollovers = client.query(SELECT_QUERY, QueryResultProcessor.forMapping(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        Assertions.assertEquals(1, StreamSupport.stream(resultAfterMultipleRollovers.spliterator(), false).count());
    }

    @Test
    public void testKeepingOldDataIfItIsTheLastPartition() throws DatabaseException {
        final Client client = getTestSubject();
        final DeleteOldRolloverStrategy testSubject = new DeleteOldRolloverStrategy(ZonedDateTime::now, 1);

        final List<Event> entries = new ArrayList<>();
        entries.add(new Event(Instant.now().minus(2, ChronoUnit.DAYS), "A", 1));

        client.execute(CREATE_EVENT_TABLE);
        client.insert(EVENT_TABLE_NAME, InsertRowDataSource.forMapping(EVENT_TABLE_INSERT_MAPPING, entries));

        testSubject.rollOver(client, EVENT_TABLE_NAME);

        final Iterable<Event> resultAfterRollover = client.query(SELECT_QUERY, QueryResultProcessor.forMapping(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));
        Assertions.assertEquals(1, StreamSupport.stream(resultAfterRollover.spliterator(), false).count());
    }

    private Client getTestSubject() {
        final CairoConfiguration configuration = new DefaultCairoConfiguration(TEST_DB_PATH.toURI().getRawPath());
        final CairoEngine engine = new CairoEngine(configuration);
        return new EmbeddedClient(() -> engine);
    }
}