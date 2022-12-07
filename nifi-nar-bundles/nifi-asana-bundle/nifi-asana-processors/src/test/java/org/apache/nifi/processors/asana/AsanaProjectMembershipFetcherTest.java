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

import com.asana.models.Project;
import com.asana.models.ProjectMembership;
import com.asana.models.User;
import com.google.api.client.util.DateTime;
import java.util.stream.Stream;
import org.apache.nifi.controller.asana.AsanaClient;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcherException;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.apache.nifi.processors.asana.utils.AsanaProjectMembershipFetcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AsanaProjectMembershipFetcherTest {

    @Mock
    private AsanaClient client;
    private Project project;

    @BeforeEach
    public void init() {
        project = new Project();
        project.gid = "123";
        project.modifiedAt = new DateTime(123456789);
        project.name = "My Project";

        when(client.getProjectByName(project.name)).thenReturn(project);
    }

    @Test
    public void testNoObjectsFetchedWhenNoProjectMembershipsReturned() {
        when(client.getProjectMemberships(any())).then(invocation -> Stream.empty());

        final AsanaObjectFetcher fetcher = new AsanaProjectMembershipFetcher(client, project.name);
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(1)).getProjectMemberships(project);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleMembershipFetched() {
        final ProjectMembership membership = new ProjectMembership();
        membership.gid = "123";
        membership.project = project;
        membership.user = new User();
        membership.user.gid = "456";
        membership.user.name = "Foo Bar";

        when(client.getProjectMemberships(any())).then(invocation -> Stream.of(membership));

        final AsanaObjectFetcher fetcher = new AsanaProjectMembershipFetcher(client, project.name);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(membership.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(1)).getProjectMemberships(project);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleMembershipUpdatedWhenAnyPartChanges() {
        final ProjectMembership membership = new ProjectMembership();
        membership.gid = "123";
        membership.project = project;
        membership.user = new User();
        membership.user.gid = "456";
        membership.user.name = "Foo Bar";

        when(client.getProjectMemberships(any())).then(invocation -> Stream.of(membership));

        final AsanaObjectFetcher fetcher = new AsanaProjectMembershipFetcher(client, project.name);
        assertNotNull(fetcher.fetchNext());
        assertNull(fetcher.fetchNext());

        membership.user.name = "Bar Foo";
        AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(membership.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(2)).getProjectMemberships(project);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testRestoreStateAndContinue() {
        final ProjectMembership membership = new ProjectMembership();
        membership.gid = "123";
        membership.project = project;
        membership.user = new User();
        membership.user.gid = "456";
        membership.user.name = "Foo Bar";

        when(client.getProjectMemberships(any())).then(invocation -> Stream.of(membership));

        final AsanaObjectFetcher fetcher1 = new AsanaProjectMembershipFetcher(client, project.name);
        assertNotNull(fetcher1.fetchNext());

        final AsanaObjectFetcher fetcher2 = new AsanaProjectMembershipFetcher(client, project.name);
        fetcher2.loadState(fetcher1.saveState());

        membership.user.name = "Bar Foo";
        AsanaObject object = fetcher2.fetchNext();

        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(membership.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(2)).getProjectMemberships(project);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testClearState() {
        final ProjectMembership membership = new ProjectMembership();
        membership.gid = "123";
        membership.project = project;
        membership.user = new User();
        membership.user.gid = "456";
        membership.user.name = "Foo Bar";

        when(client.getProjectMemberships(any())).then(invocation -> Stream.of(membership));

        final AsanaObjectFetcher fetcher = new AsanaProjectMembershipFetcher(client, project.name);
        assertNotNull(fetcher.fetchNext());

        membership.user.name = "Bar Foo";
        fetcher.clearState();
        AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(membership.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(2)).getProjectMemberships(project);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testWrongStateForConfigurationThrows() {
        final Project otherProject = new Project();
        otherProject.gid = "999";
        otherProject.name = "Other Project";

        when(client.getProjectByName(otherProject.name)).thenReturn(otherProject);

        final AsanaObjectFetcher fetcher1 = new AsanaProjectMembershipFetcher(client, project.name);
        final AsanaObjectFetcher fetcher2 = new AsanaProjectMembershipFetcher(client, otherProject.name);
        assertThrows(AsanaObjectFetcherException.class, () -> fetcher2.loadState(fetcher1.saveState()));
    }
}
