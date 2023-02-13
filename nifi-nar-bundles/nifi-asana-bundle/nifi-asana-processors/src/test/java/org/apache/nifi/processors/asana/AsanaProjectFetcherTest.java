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

import com.asana.models.Project;
import com.google.api.client.util.DateTime;
import java.util.stream.Stream;
import org.apache.nifi.controller.asana.AsanaClient;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.apache.nifi.processors.asana.utils.AsanaProjectFetcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AsanaProjectFetcherTest {

    @Mock
    private AsanaClient client;

    @Test
    public void testNoProjectsFetchedWhenNoProjectsReturned() {
        when(client.getProjects()).then(invocation -> Stream.empty());

        final AsanaObjectFetcher fetcher = new AsanaProjectFetcher(client);
        assertNull(fetcher.fetchNext());

        verify(client, times(1)).getProjects();
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleProjectFetched() {
        final Project project = new Project();
        project.gid = "123";
        project.name = "My Project";
        project.modifiedAt = new DateTime(123456789);

        when(client.getProjects()).then(invocation -> Stream.of(project));

        final AsanaObjectFetcher fetcher = new AsanaProjectFetcher(client);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(project.gid, object.getGid());
        verify(client, times(1)).getProjects();
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleProjectUpdatedOnlyWhenModificationDateChanges() {
        final Project project = new Project();
        project.gid = "123";
        project.name = "My Project";
        project.modifiedAt = new DateTime(123456789);

        when(client.getProjects()).then(invocation -> Stream.of(project));

        final AsanaObjectFetcher fetcher = new AsanaProjectFetcher(client);
        assertNotNull(fetcher.fetchNext());
        assertNull(fetcher.fetchNext());

        project.name = "My Project Is Renamed";
        assertNull(fetcher.fetchNext());

        project.modifiedAt = new DateTime(234567891);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(project.gid, object.getGid());
        verify(client, times(3)).getProjects();
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testClearState() {
        final Project project = new Project();
        project.gid = "123";
        project.name = "My Project";
        project.modifiedAt = new DateTime(123456789);

        when(client.getProjects()).then(invocation -> Stream.of(project));

        final AsanaObjectFetcher fetcher = new AsanaProjectFetcher(client);
        assertNotNull(fetcher.fetchNext());

        project.modifiedAt = new DateTime(234567891);
        fetcher.clearState();

        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(project.gid, object.getGid());
        verify(client, times(2)).getProjects();
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testRestoreStateAndContinue() {
        final Project project = new Project();
        project.gid = "123";
        project.name = "My Project";
        project.modifiedAt = new DateTime(123456789);

        when(client.getProjects()).then(invocation -> Stream.of(project));

        final AsanaObjectFetcher fetcher1 = new AsanaProjectFetcher(client);
        assertNotNull(fetcher1.fetchNext());
        assertNull(fetcher1.fetchNext());

        project.name = "My Project Is Renamed";
        assertNull(fetcher1.fetchNext());

        project.modifiedAt = new DateTime(234567891);

        final AsanaObjectFetcher fetcher2 = new AsanaProjectFetcher(client);
        fetcher2.loadState(fetcher1.saveState());
        AsanaObject object = fetcher2.fetchNext();

        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(project.gid, object.getGid());
        verify(client, times(3)).getProjects();
        verifyNoMoreInteractions(client);
    }
}
