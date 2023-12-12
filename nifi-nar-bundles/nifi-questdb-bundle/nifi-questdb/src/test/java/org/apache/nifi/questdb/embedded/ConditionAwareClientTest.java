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

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class ConditionAwareClientTest {

    @Mock
    Client client;

    @Test
    public void testClientIsCalledWhenConditionAllows() throws DatabaseException {
        final ConditionAwareClient testSubject = new ConditionAwareClient(() -> true, client);

        testSubject.execute(QuestDbTestUtil.SELECT_QUERY);
        verify(client, times(1)).execute(QuestDbTestUtil.SELECT_QUERY);

        final QueryResultProcessor<List<Event>> queryResultProcessor = RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING);
        testSubject.query(QuestDbTestUtil.SELECT_QUERY, queryResultProcessor);
        verify(client, times(1)).query(QuestDbTestUtil.SELECT_QUERY, queryResultProcessor);

        final InsertRowDataSource insertRowDataSource = QuestDbTestUtil.getEventTableDataSource(Collections.emptyList());
        testSubject.insert(QuestDbTestUtil.EVENT_TABLE_NAME, insertRowDataSource);
        verify(client, times(1)).insert(QuestDbTestUtil.EVENT_TABLE_NAME, insertRowDataSource);
    }

    @Test
    public void testClientIsNotCalledWhenConditionDisallows() throws DatabaseException {
        final ConditionAwareClient testSubject = new ConditionAwareClient(() -> false, client);

        assertThrows(DatabaseException.class, () -> testSubject.execute(QuestDbTestUtil.SELECT_QUERY));
        verify(client, never()).execute(anyString());

        assertThrows(DatabaseException.class, () ->  testSubject.query(QuestDbTestUtil.SELECT_QUERY, RequestMapping.getResultProcessor(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING)));
        verify(client, never()).query(anyString(), any(QueryResultProcessor.class));

        assertThrows(DatabaseException.class, () -> testSubject.insert(QuestDbTestUtil.EVENT_TABLE_NAME, QuestDbTestUtil.getEventTableDataSource(Collections.emptyList())));
        verify(client, never()).insert(anyString(), any(InsertRowDataSource.class));
    }
}
