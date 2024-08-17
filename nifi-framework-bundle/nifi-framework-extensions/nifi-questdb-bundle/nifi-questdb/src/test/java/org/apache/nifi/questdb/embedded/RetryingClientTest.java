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
import org.apache.nifi.questdb.InsertRowDataSource;
import org.apache.nifi.questdb.QueryResultProcessor;
import org.apache.nifi.questdb.mapping.RequestMapping;
import org.apache.nifi.questdb.util.Event;
import org.apache.nifi.questdb.util.QuestDbTestUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class RetryingClientTest {

    @Mock
    Client fallbackClient;

    private TestClient testQuestDbClient;

    private Client testSubject;

    @Test
    public void testCompileRealClientIsCalledOnceWhenNoError() throws DatabaseException {
        setUpTestSubject(0);

        testSubject.execute(QuestDbTestUtil.SELECT_QUERY);

        assertEquals(1, testQuestDbClient.getNumberOfCalls());
        verify(fallbackClient, never()).execute(anyString());
    }

    @Test
    public void testCompileRealClientIsCalledTwiceWhenThereIsError() throws DatabaseException {
        setUpTestSubject(1);

        testSubject.execute(QuestDbTestUtil.SELECT_QUERY);

        assertEquals(2, testQuestDbClient.getNumberOfCalls());
        verify(fallbackClient, never()).execute(anyString());
    }

    @Test
    public void testCompileFallbackIsCalledWhenErrorRemains() throws DatabaseException {
        setUpTestSubject(3);

        testSubject.execute(QuestDbTestUtil.SELECT_QUERY);

        assertEquals(3, testQuestDbClient.getNumberOfCalls());
        verify(fallbackClient, times(1)).execute(QuestDbTestUtil.SELECT_QUERY);
    }

    @Test
    public void testQueryRealClientIsCalledOnceWhenNoError() throws DatabaseException {
        setUpTestSubject(0);

        testSubject.query(QuestDbTestUtil.SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));

        assertEquals(1, testQuestDbClient.getNumberOfCalls());
        verify(fallbackClient, never()).query(anyString(), any(QueryResultProcessor.class));
    }

    @Test
    public void testQueryRealClientIsCalledTwiceWhenThereIsError() throws DatabaseException {
        setUpTestSubject(1);

        testSubject.query(QuestDbTestUtil.SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING));

        assertEquals(2, testQuestDbClient.getNumberOfCalls());
        verify(fallbackClient, never()).query(anyString(), any(QueryResultProcessor.class));
    }

    @Test
    public void testQueryFallbackIsCalledWhenErrorRemains() throws DatabaseException {
        setUpTestSubject(3);

        final QueryResultProcessor<List<Event>> queryResultProcessor = RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING);
        testSubject.query(QuestDbTestUtil.SELECT_QUERY, queryResultProcessor);

        assertEquals(3, testQuestDbClient.getNumberOfCalls());
        verify(fallbackClient, times(1)).query(QuestDbTestUtil.SELECT_QUERY, queryResultProcessor);
    }

    @Test
    public void testInsertRealClientIsCalledOnceWhenNoError() throws DatabaseException {
        setUpTestSubject(0);
        final List<Event> events = QuestDbTestUtil.getTestData();

        testSubject.insert(QuestDbTestUtil.EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(events));

        assertEquals(1, testQuestDbClient.getNumberOfCalls());
        verify(fallbackClient, never()).insert(anyString(), any(InsertRowDataSource.class));
    }

    @Test
    public void testInsertRealClientIsCalledTwiceWhenThereIsError() throws DatabaseException {
        setUpTestSubject(1);
        final List<Event> events = QuestDbTestUtil.getTestData();

        testSubject.insert(QuestDbTestUtil.EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(events));

        assertEquals(2, testQuestDbClient.getNumberOfCalls());
        verify(fallbackClient, never()).insert(anyString(), any(InsertRowDataSource.class));
    }

    @Test
    public void testInsertFallbackIsCalledWhenErrorRemains() throws DatabaseException {
        setUpTestSubject(3);
        final List<Event> events = QuestDbTestUtil.getTestData();

        final InsertRowDataSource insertRowDataSource = QuestDbTestUtil.getEventTableDataSource(events);
        testSubject.insert(QuestDbTestUtil.EVENT_TABLE_NAME, insertRowDataSource);

        assertEquals(3, testQuestDbClient.getNumberOfCalls());
        verify(fallbackClient, times(1)).insert(QuestDbTestUtil.EVENT_TABLE_NAME, insertRowDataSource);
    }

    private void setUpTestSubject(final int numberOfErrors) {
        testQuestDbClient = new TestClient(numberOfErrors);
        testSubject = RetryingClient.getInstance(2, (i, e) -> { }, testQuestDbClient, fallbackClient);
    }

    private static class TestClient implements Client {
        private final int numberOfErrors;
        private int numberOfCalls = 0;

        private TestClient(final int numberOfErrors) {
            this.numberOfErrors = numberOfErrors;
        }

        @Override
        public void execute(final String query) throws DatabaseException {
            numberOfCalls++;

            if (numberOfCalls <= numberOfErrors) {
                throw new DatabaseException("Test exception");
            }
        }

        @Override
        public void insert(final String tableName, final InsertRowDataSource rowSource) throws DatabaseException {
            numberOfCalls++;

            if (numberOfCalls <= numberOfErrors) {
                throw new DatabaseException("Test exception");
            }
        }

        @Override
        public <T> T query(final String query, final QueryResultProcessor<T> rowProcessor) throws DatabaseException {
            numberOfCalls++;

            if (numberOfCalls <= numberOfErrors) {
                throw new DatabaseException("Test exception");
            }

            return rowProcessor.getResult();
        }

        @Override
        public void disconnect() { }

        public int getNumberOfCalls() {
            return numberOfCalls;
        }
    }
}
