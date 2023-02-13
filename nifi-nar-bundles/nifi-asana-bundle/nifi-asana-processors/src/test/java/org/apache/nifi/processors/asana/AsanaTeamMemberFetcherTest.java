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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.asana.models.Team;
import com.asana.models.User;
import java.util.stream.Stream;
import org.apache.nifi.controller.asana.AsanaClient;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.apache.nifi.processors.asana.utils.AsanaTeamMemberFetcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AsanaTeamMemberFetcherTest {

    @Mock
    private AsanaClient client;
    private Team team;

    @BeforeEach
    public void init() {
        team = new Team();
        team.gid = "123";
        team.name = "My Team";
        team.description = "Lorem Ipsum";

        when(client.getTeamByName(team.name)).thenReturn(team);
    }

    @Test
    public void testNoObjectsFetchedWhenNoTeamMembersReturned() {
        when(client.getTeamMembers(any())).then(invocation -> Stream.empty());

        final AsanaObjectFetcher fetcher = new AsanaTeamMemberFetcher(client, team.name);
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getTeamByName(team.name);
        verify(client, times(1)).getTeamMembers(team);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleMemberFetched() {
        final User user = new User();
        user.gid = "123";
        user.name = "My User";
        user.email = "myuser@example.com";

        when(client.getTeamMembers(any())).then(invocation -> Stream.of(user));

        final AsanaObjectFetcher fetcher = new AsanaTeamMemberFetcher(client, team.name);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(user.gid, object.getGid());

        verify(client, atLeastOnce()).getTeamByName(team.name);
        verify(client, times(1)).getTeamMembers(team);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleMemberUpdatedWhenAnyPartChanges() {
        final User user = new User();
        user.gid = "123";
        user.name = "My User";
        user.email = "myuser@example.com";

        when(client.getTeamMembers(any())).then(invocation -> Stream.of(user));

        final AsanaObjectFetcher fetcher = new AsanaTeamMemberFetcher(client, team.name);
        assertNotNull(fetcher.fetchNext());
        assertNull(fetcher.fetchNext());

        user.name = "Bar Foo";
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(user.gid, object.getGid());

        verify(client, atLeastOnce()).getTeamByName(team.name);
        verify(client, times(2)).getTeamMembers(team);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testRestoreStateAndContinue() {
        final User user = new User();
        user.gid = "123";
        user.name = "My User";
        user.email = "myuser@example.com";

        when(client.getTeamMembers(any())).then(invocation -> Stream.of(user));

        final AsanaObjectFetcher fetcher1 = new AsanaTeamMemberFetcher(client, team.name);
        assertNotNull(fetcher1.fetchNext());

        final AsanaObjectFetcher fetcher2 = new AsanaTeamMemberFetcher(client, team.name);
        fetcher2.loadState(fetcher1.saveState());

        user.name = "Bar Foo";
        final AsanaObject object = fetcher2.fetchNext();

        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(user.gid, object.getGid());

        verify(client, atLeastOnce()).getTeamByName(team.name);
        verify(client, times(2)).getTeamMembers(team);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testClearState() {
        final User user = new User();
        user.gid = "123";
        user.name = "My User";
        user.email = "myuser@example.com";

        when(client.getTeamMembers(any())).then(invocation -> Stream.of(user));

        final AsanaObjectFetcher fetcher = new AsanaTeamMemberFetcher(client, team.name);
        assertNotNull(fetcher.fetchNext());

        fetcher.clearState();

        user.name = "Bar Foo";
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(user.gid, object.getGid());

        verify(client, atLeastOnce()).getTeamByName(team.name);
        verify(client, times(2)).getTeamMembers(team);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testWrongStateForConfigurationThrows() {
        final Team otherTeam = new Team();
        team.gid = "999";
        team.name = "My Second Team";
        team.description = "Bla bla";

        when(client.getTeamByName(otherTeam.name)).thenReturn(otherTeam);

        final AsanaObjectFetcher fetcher1 = new AsanaTeamMemberFetcher(client, team.name);
        final AsanaObjectFetcher fetcher2 = new AsanaTeamMemberFetcher(client, otherTeam.name);
        assertThrows(RuntimeException.class, () -> fetcher2.loadState(fetcher1.saveState()));
    }
}
