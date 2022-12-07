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

import com.asana.models.Team;
import java.util.stream.Stream;
import org.apache.nifi.controller.asana.AsanaClient;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.apache.nifi.processors.asana.utils.AsanaTeamFetcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AsanaTeamFetcherTest {

    @Mock
    private AsanaClient client;

    @Test
    public void testNoObjectsFetchedWhenNoTeamsReturned() {
        when(client.getTeams()).then(invocation -> Stream.empty());

        final AsanaObjectFetcher fetcher = new AsanaTeamFetcher(client);
        assertNull(fetcher.fetchNext());

        verify(client, times(1)).getTeams();
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleTeamFetched() {
        final Team team = new Team();
        team.gid = "123";
        team.name = "My Team";
        team.description = "Lorem Ipsum";

        when(client.getTeams()).then(invocation -> Stream.of(team));

        final AsanaObjectFetcher fetcher = new AsanaTeamFetcher(client);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(team.gid, object.getGid());
        verify(client, times(1)).getTeams();
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleTeamUpdatedWhenAnyPartChanges() {
        final Team team = new Team();
        team.gid = "123";
        team.name = "My Team";
        team.description = "Lorem Ipsum";

        when(client.getTeams()).then(invocation -> Stream.of(team));

        final AsanaObjectFetcher fetcher = new AsanaTeamFetcher(client);
        assertNotNull(fetcher.fetchNext());
        assertNull(fetcher.fetchNext());

        team.description = "Bla bla";
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(team.gid, object.getGid());
        verify(client, times(2)).getTeams();
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testClearState() {
        final Team team = new Team();
        team.gid = "123";
        team.name = "My Team";
        team.description = "Lorem Ipsum";

        when(client.getTeams()).then(invocation -> Stream.of(team));

        final AsanaObjectFetcher fetcher = new AsanaTeamFetcher(client);
        assertNotNull(fetcher.fetchNext());

        fetcher.clearState();

        team.description = "Bla bla";
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(team.gid, object.getGid());
        verify(client, times(2)).getTeams();
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testRestoreStateAndContinue() {
        final Team team = new Team();
        team.gid = "123";
        team.name = "My Team";
        team.description = "Lorem Ipsum";

        when(client.getTeams()).then(invocation -> Stream.of(team));

        final AsanaObjectFetcher fetcher1 = new AsanaTeamFetcher(client);
        assertNotNull(fetcher1.fetchNext());

        final AsanaObjectFetcher fetcher2 = new AsanaTeamFetcher(client);
        fetcher2.loadState(fetcher1.saveState());

        team.description = "Bla bla";
        final AsanaObject object = fetcher2.fetchNext();

        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(team.gid, object.getGid());
        verify(client, times(2)).getTeams();
        verifyNoMoreInteractions(client);
    }
}
