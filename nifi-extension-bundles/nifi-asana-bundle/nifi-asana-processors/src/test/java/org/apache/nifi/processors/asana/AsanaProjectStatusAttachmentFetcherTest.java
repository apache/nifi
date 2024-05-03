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

import com.asana.models.Attachment;
import com.asana.models.Project;
import com.asana.models.ProjectStatus;
import com.google.api.client.util.DateTime;
import java.util.stream.Stream;
import org.apache.nifi.controller.asana.AsanaClient;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcherException;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.apache.nifi.processors.asana.utils.AsanaProjectStatusAttachmentFetcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class AsanaProjectStatusAttachmentFetcherTest {

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
    public void testNoAttachmentsFetchedWhenNoStatusUpdatesReturned() {
        when(client.getProjectStatusUpdates(any())).then(invocation -> Stream.empty());

        final AsanaObjectFetcher fetcher = new AsanaProjectStatusAttachmentFetcher(client, project.name);
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(1)).getProjectStatusUpdates(project);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testNoAttachmentsFetchedWhenNoAttachmentsReturned() {
        final ProjectStatus status = new ProjectStatus();
        status.gid = "123";

        when(client.getProjectStatusUpdates(any())).then(invocation -> Stream.of(status));
        when(client.getAttachments(any(ProjectStatus.class))).then(invocation -> Stream.empty());

        final AsanaObjectFetcher fetcher = new AsanaProjectStatusAttachmentFetcher(client, project.name);
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(1)).getProjectStatusUpdates(project);
        verify(client, times(1)).getAttachments(status);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleAttachmentFetched() {
        final ProjectStatus status = new ProjectStatus();
        status.gid = "123";

        final Attachment attachment = new Attachment();
        attachment.gid = "456";
        attachment.createdAt = new DateTime(123456789);
        attachment.name = "foo.txt";

        when(client.getProjectStatusUpdates(any())).then(invocation -> Stream.of(status));
        when(client.getAttachments(any(ProjectStatus.class))).then(invocation -> Stream.of(attachment));

        final AsanaObjectFetcher fetcher = new AsanaProjectStatusAttachmentFetcher(client, project.name);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(attachment.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(1)).getProjectStatusUpdates(project);
        verify(client, times(1)).getAttachments(status);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testAttachmentUpdateIsNotDetected() {
        final ProjectStatus status = new ProjectStatus();
        status.gid = "123";

        final Attachment attachment = new Attachment();
        attachment.gid = "456";
        attachment.createdAt = new DateTime(123456789);
        attachment.name = "foo.txt";

        when(client.getProjectStatusUpdates(any())).then(invocation -> Stream.of(status));
        when(client.getAttachments(any(ProjectStatus.class))).then(invocation -> Stream.of(attachment));

        final AsanaObjectFetcher fetcher = new AsanaProjectStatusAttachmentFetcher(client, project.name);
        assertNotNull(fetcher.fetchNext());
        assertNull(fetcher.fetchNext());

        attachment.name = "bar.txt";
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(2)).getProjectStatusUpdates(project);
        verify(client, times(2)).getAttachments(status);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testAttachmentRemovalIsDetected() {
        final ProjectStatus status = new ProjectStatus();
        status.gid = "123";

        final Attachment attachment = new Attachment();
        attachment.gid = "456";
        attachment.createdAt = new DateTime(123456789);
        attachment.name = "foo.txt";

        when(client.getProjectStatusUpdates(any())).then(invocation -> Stream.of(status));
        when(client.getAttachments(any(ProjectStatus.class))).then(invocation -> Stream.of(attachment));

        final AsanaObjectFetcher fetcher = new AsanaProjectStatusAttachmentFetcher(client, project.name);
        assertNotNull(fetcher.fetchNext());
        assertNull(fetcher.fetchNext());

        when(client.getAttachments(any(ProjectStatus.class))).then(invocation -> Stream.empty());
        AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.REMOVED, object.getState());
        assertEquals(attachment.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(2)).getProjectStatusUpdates(project);
        verify(client, times(2)).getAttachments(status);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testRestoreStateAndContinue() {
        final ProjectStatus status = new ProjectStatus();
        status.gid = "123";

        final Attachment attachment = new Attachment();
        attachment.gid = "456";
        attachment.createdAt = new DateTime(123456789);
        attachment.name = "foo.txt";

        when(client.getProjectStatusUpdates(any())).then(invocation -> Stream.of(status));
        when(client.getAttachments(any(ProjectStatus.class))).then(invocation -> Stream.of(attachment));

        final AsanaObjectFetcher fetcher1 = new AsanaProjectStatusAttachmentFetcher(client, project.name);
        assertNotNull(fetcher1.fetchNext());
        assertNull(fetcher1.fetchNext());

        final AsanaObjectFetcher fetcher2 = new AsanaProjectStatusAttachmentFetcher(client, project.name);
        fetcher2.loadState(fetcher1.saveState());

        when(client.getAttachments(any(ProjectStatus.class))).then(invocation -> Stream.empty());
        AsanaObject object = fetcher2.fetchNext();

        assertEquals(AsanaObjectState.REMOVED, object.getState());
        assertEquals(attachment.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(2)).getProjectStatusUpdates(project);
        verify(client, times(2)).getAttachments(status);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testClearState() {
        final ProjectStatus status = new ProjectStatus();
        status.gid = "123";

        Attachment attachment = new Attachment();
        attachment.gid = "456";
        attachment.createdAt = new DateTime(123456789);
        attachment.name = "foo.txt";

        when(client.getProjectStatusUpdates(any())).then(invocation -> Stream.of(status));
        when(client.getAttachments(any(ProjectStatus.class))).then(invocation -> Stream.of(attachment));

        final AsanaObjectFetcher fetcher = new AsanaProjectStatusAttachmentFetcher(client, project.name);
        assertNotNull(fetcher.fetchNext());
        assertNull(fetcher.fetchNext());

        attachment.name = "bar.txt";
        fetcher.clearState();

        final AsanaObject object = fetcher.fetchNext();
        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(attachment.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(2)).getProjectStatusUpdates(project);
        verify(client, times(2)).getAttachments(status);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testWrongStateForConfigurationThrows() {
        final Project otherProject = new Project();
        otherProject.gid = "999";
        otherProject.name = "Other Project";

        when(client.getProjectByName(otherProject.name)).thenReturn(otherProject);

        final AsanaObjectFetcher fetcher1 = new AsanaProjectStatusAttachmentFetcher(client, project.name);
        final AsanaObjectFetcher fetcher2 = new AsanaProjectStatusAttachmentFetcher(client, otherProject.name);
        assertThrows(AsanaObjectFetcherException.class, () -> fetcher2.loadState(fetcher1.saveState()));
    }
}
