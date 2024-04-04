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

import com.asana.models.Event;
import com.asana.models.Project;
import com.google.api.client.util.DateTime;
import org.apache.nifi.controller.asana.AsanaClient;
import org.apache.nifi.controller.asana.AsanaEventsCollection;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcherException;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.apache.nifi.processors.asana.utils.AsanaProjectEventFetcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class AsanaProjectEventFetcherTest {

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
    public void testSyncTokenIsUpdated() {
        final AsanaObjectFetcher fetcher = new AsanaProjectEventFetcher(client, project.name);

        when(client.getEvents(any(), any()))
            .thenReturn(new AsanaEventsCollection("foo", emptyList()))
            .thenReturn(new AsanaEventsCollection("bar", emptyList()));

        fetcher.fetchNext();
        fetcher.fetchNext();
        fetcher.fetchNext();

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(1)).getEvents(project, "");
        verify(client, times(1)).getEvents(project, "foo");
        verify(client, times(1)).getEvents(project, "bar");
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleEventIsFetched() {
        final AsanaObjectFetcher fetcher = new AsanaProjectEventFetcher(client, project.name);

        final Event event = new Event();
        event.createdAt = new DateTime(123456789);
        event.type = "SomeEvent";
        event.resource = event.new Entity();
        event.resource.name = "Foo Bar";
        event.resource.resourceType = "Something";
        event.resource.gid = "123";

        when(client.getEvents(any(), any()))
            .thenReturn(new AsanaEventsCollection("foo", singletonList(event)))
            .thenReturn(new AsanaEventsCollection("bar", emptyList()));

        final AsanaObject object = fetcher.fetchNext();
        assertEquals(AsanaObjectState.NEW, object.getState());
        assertFalse(object.getGid().isEmpty());
        assertFalse(object.getContent().isEmpty());
        assertFalse(object.getFingerprint().isEmpty());
    }

    @Test
    public void testRestoreStateAndContinue() {
        final AsanaObjectFetcher fetcher1 = new AsanaProjectEventFetcher(client, project.name);

        when(client.getEvents(any(), any()))
                .thenReturn(new AsanaEventsCollection("foo", emptyList()))
                .thenReturn(new AsanaEventsCollection("bar", emptyList()));

        fetcher1.fetchNext();
        fetcher1.fetchNext();

        final AsanaObjectFetcher fetcher2 = new AsanaProjectEventFetcher(client, project.name);
        fetcher2.loadState(fetcher1.saveState());

        fetcher2.fetchNext();

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(1)).getEvents(project, "");
        verify(client, times(1)).getEvents(project, "foo");
        verify(client, times(1)).getEvents(project, "bar");
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testClearState() {
        final AsanaObjectFetcher fetcher = new AsanaProjectEventFetcher(client, project.name);

        when(client.getEvents(any(), any()))
                .thenReturn(new AsanaEventsCollection("foo", emptyList()))
                .thenReturn(new AsanaEventsCollection("bar", emptyList()));

        fetcher.fetchNext();
        fetcher.fetchNext();
        fetcher.clearState();
        fetcher.fetchNext();

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(2)).getEvents(project, "");
        verify(client, times(1)).getEvents(project, "foo");
        verify(client, times(0)).getEvents(project, "bar");
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testWrongStateForConfigurationThrows() {
        final AsanaObjectFetcher fetcher1 = new AsanaProjectEventFetcher(client, project.name);

        Project otherProject = new Project();
        otherProject.name = "Other Project";
        otherProject.gid = "999";

        when(client.getProjectByName(otherProject.name)).thenReturn(otherProject);

        final AsanaObjectFetcher fetcher2 = new AsanaProjectEventFetcher(client, otherProject.name);
        assertThrows(AsanaObjectFetcherException.class, () -> fetcher2.loadState(fetcher1.saveState()));
    }
}
