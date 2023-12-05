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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

@ExtendWith(MockitoExtension.class)
public class ConditionAwareClientTest {

    @Mock
    Client client;

    @Test
    public void testClientIsCalledWhenConditionAllows() throws DatabaseException {
        final ConditionAwareClient testSubject = new ConditionAwareClient(() -> true, client);

        testSubject.execute(QuestDbTestUtil.SELECT_QUERY);
        Mockito.verify(client, Mockito.times(1)).execute(QuestDbTestUtil.SELECT_QUERY);

        final QueryResultProcessor<List<Event>> queryResultProcessor = QueryResultProcessor.forMapping(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING);
        testSubject.query(QuestDbTestUtil.SELECT_QUERY, queryResultProcessor);
        Mockito.verify(client, Mockito.times(1)).query(QuestDbTestUtil.SELECT_QUERY, queryResultProcessor);


        final InsertRowDataSource insertRowDataSource = InsertRowDataSource.forMapping(QuestDbTestUtil.EVENT_TABLE_INSERT_MAPPING, Collections.emptyList());
        testSubject.insert(QuestDbTestUtil.EVENT_TABLE_NAME, insertRowDataSource);
        Mockito.verify(client, Mockito.times(1)).insert(QuestDbTestUtil.EVENT_TABLE_NAME, insertRowDataSource);
    }

    @Test
    public void testClientIsNotCalledWhenConditionDisallows() throws DatabaseException {
        final ConditionAwareClient testSubject = new ConditionAwareClient(() -> false, client);

        Assertions.assertThrows(DatabaseException.class, () -> testSubject.execute(QuestDbTestUtil.SELECT_QUERY));
        Mockito.verify(client, Mockito.never()).execute(Mockito.anyString());

        Assertions.assertThrows(DatabaseException.class, () ->  testSubject.query(QuestDbTestUtil.SELECT_QUERY, QueryResultProcessor.forMapping(QuestDbTestUtil.EVENT_TABLE_REQUEST_MAPPING)));
        Mockito.verify(client, Mockito.never()).query(Mockito.anyString(), Mockito.any(QueryResultProcessor.class));

        Assertions.assertThrows(
            DatabaseException.class,
            () -> testSubject.insert(QuestDbTestUtil.EVENT_TABLE_NAME, InsertRowDataSource.forMapping(QuestDbTestUtil.EVENT_TABLE_INSERT_MAPPING, Collections.emptyList()))
        );
        Mockito.verify(client, Mockito.never()).insert(Mockito.anyString(), Mockito.any(InsertRowDataSource.class));
    }
}
