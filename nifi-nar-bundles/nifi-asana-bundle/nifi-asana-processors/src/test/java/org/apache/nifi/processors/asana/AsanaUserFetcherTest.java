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
package org.apache.nifi.processors.asana;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.asana.models.User;
import java.util.stream.Stream;
import org.apache.nifi.controller.asana.AsanaClient;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.apache.nifi.processors.asana.utils.AsanaUserFetcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AsanaUserFetcherTest {

    @Mock
    private AsanaClient client;

    @Test
    public void testNoObjectsFetchedWhenNoTeamsReturned() {
        when(client.getUsers()).then(invocation -> Stream.empty());

        final AsanaObjectFetcher fetcher = new AsanaUserFetcher(client);
        assertNull(fetcher.fetchNext());

        verify(client, times(1)).getUsers();
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleUserFetched() {
        final User user = new User();
        user.gid = "123";
        user.name = "My User";
        user.email = "myuser@example.com";

        when(client.getUsers()).then(invocation -> Stream.of(user));

        final AsanaObjectFetcher fetcher = new AsanaUserFetcher(client);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(user.gid, object.getGid());
        verify(client, times(1)).getUsers();
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleTeamUpdatedWhenAnyPartChanges() {
        final User user = new User();
        user.gid = "123";
        user.name = "My User";
        user.email = "myuser@example.com";

        when(client.getUsers()).then(invocation -> Stream.of(user));

        final AsanaObjectFetcher fetcher = new AsanaUserFetcher(client);
        assertNotNull(fetcher.fetchNext());
        assertNull(fetcher.fetchNext());

        user.email = "otheraddress@ecample.com";
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(user.gid, object.getGid());
        verify(client, times(2)).getUsers();
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testClearState() {
        final User user = new User();
        user.gid = "123";
        user.name = "My User";
        user.email = "myuser@example.com";

        when(client.getUsers()).then(invocation -> Stream.of(user));

        final AsanaObjectFetcher fetcher = new AsanaUserFetcher(client);
        assertNotNull(fetcher.fetchNext());

        fetcher.clearState();

        user.email = "otheraddress@ecample.com";
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(user.gid, object.getGid());
        verify(client, times(2)).getUsers();
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testRestoreStateAndContinue() {
        final User user = new User();
        user.gid = "123";
        user.name = "My User";
        user.email = "myuser@example.com";

        when(client.getUsers()).then(invocation -> Stream.of(user));

        final AsanaObjectFetcher fetcher1 = new AsanaUserFetcher(client);
        assertNotNull(fetcher1.fetchNext());

        final AsanaObjectFetcher fetcher2 = new AsanaUserFetcher(client);
        fetcher2.loadState(fetcher1.saveState());

        user.email = "otheraddress@ecample.com";
        final AsanaObject object = fetcher2.fetchNext();

        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(user.gid, object.getGid());
        verify(client, times(2)).getUsers();
        verifyNoMoreInteractions(client);
    }
}
