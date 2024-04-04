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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
import com.asana.models.Section;
import com.asana.models.Tag;
import com.asana.models.Task;
import com.google.api.client.util.DateTime;
import java.util.stream.Stream;
import org.apache.nifi.controller.asana.AsanaClient;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcherException;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.apache.nifi.processors.asana.utils.AsanaTaskFetcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AsanaTaskFetcherTest {

    @Mock
    private AsanaClient client;
    private Project project;
    private Section section;
    private Tag tag;

    @BeforeEach
    public void init() {
        project = new Project();
        project.gid = "123";
        project.modifiedAt = new DateTime(123456789);
        project.name = "My Project";

        when(client.getProjectByName(project.name)).thenReturn(project);

        section = new Section();
        section.gid = "456";
        section.project = project;
        section.name = "Some section";
        section.createdAt = new DateTime(123456789);

        when(client.getSections(project)).then(invocation -> Stream.of(section));
        when(client.getSectionByName(project, section.name)).thenReturn(section);

        tag = new Tag();
        tag.gid = "9876";
        tag.name = "Foo";
        tag.createdAt = new DateTime(123456789);

        when(client.getTags()).then(invocation -> Stream.of(tag));
    }

    @Test
    public void testNoObjectsFetchedWhenNoTasksReturned() {
        when(client.getTasks(any(Project.class))).then(invocation -> Stream.empty());

        final AsanaObjectFetcher fetcher = new AsanaTaskFetcher(client, project.name, null, null);
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(1)).getTasks(project);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testNoObjectsFetchedWhenNoTasksReturnedBySection() {
        when(client.getTasks(any(Section.class))).then(invocation -> Stream.empty());

        final AsanaObjectFetcher fetcher = new AsanaTaskFetcher(client, project.name, section.name, null);
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getSectionByName(project, section.name);
        verify(client, times(1)).getTasks(section);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testNoObjectsFetchedWhenNoTasksReturnedByTag() {
        when(client.getTasks(any(Project.class))).then(invocation -> Stream.empty());

        final AsanaObjectFetcher fetcher = new AsanaTaskFetcher(client, project.name, null, tag.name);
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getTags();
        verify(client, times(1)).getTasks(project);
        verify(client, times(1)).getTasks(tag);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleTaskFetched() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        when(client.getTasks(any(Project.class))).then(invocation -> Stream.of(task));

        final AsanaObjectFetcher fetcher = new AsanaTaskFetcher(client, project.name, null, null);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(task.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(1)).getTasks(project);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleTaskFetchedBySection() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        when(client.getTasks(any(Section.class))).then(invocation -> Stream.of(task));

        final AsanaObjectFetcher fetcher = new AsanaTaskFetcher(client, project.name, section.name, null);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(task.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getSectionByName(project, section.name);
        verify(client, times(1)).getTasks(section);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleTaskFetchedByTag() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        when(client.getTasks(any(Project.class))).then(invocation -> Stream.of(task));
        when(client.getTasks(any(Tag.class))).then(invocation -> Stream.of(task));

        final AsanaObjectFetcher fetcher = new AsanaTaskFetcher(client, project.name, null, tag.name);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(task.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getTags();
        verify(client, times(1)).getTasks(project);
        verify(client, times(1)).getTasks(tag);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testNoTaskFetchedByNonMatchingTag() {
        final Task task1 = new Task();
        task1.gid = "1234";
        task1.name = "My first task";
        task1.modifiedAt = new DateTime(123456789);

        final Task task2 = new Task();
        task2.gid = "5678";
        task2.name = "My other task";
        task2.modifiedAt = new DateTime(123456789);

        when(client.getTasks(any(Project.class))).then(invocation -> Stream.of(task1));
        when(client.getTasks(any(Tag.class))).then(invocation -> Stream.of(task2));

        final AsanaObjectFetcher fetcher = new AsanaTaskFetcher(client, project.name, null, tag.name);
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getTags();
        verify(client, times(1)).getTasks(project);
        verify(client, times(1)).getTasks(tag);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testTaskRemovedFromSection() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        when(client.getTasks(any(Section.class))).then(invocation -> Stream.of(task));

        final AsanaObjectFetcher fetcher = new AsanaTaskFetcher(client, project.name, section.name, null);
        assertNotNull(fetcher.fetchNext());
        assertNull(fetcher.fetchNext());

        when(client.getTasks(any(Section.class))).then(invocation -> Stream.empty());

        final AsanaObject object = fetcher.fetchNext();
        assertEquals(AsanaObjectState.REMOVED, object.getState());
        assertEquals(task.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getSectionByName(project, section.name);
        verify(client, times(2)).getTasks(section);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testTaskUntagged() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        when(client.getTasks(any(Project.class))).then(invocation -> Stream.of(task));
        when(client.getTasks(any(Tag.class))).then(invocation -> Stream.of(task));

        final AsanaObjectFetcher fetcher = new AsanaTaskFetcher(client, project.name, null, tag.name);
        assertNotNull(fetcher.fetchNext());
        assertNull(fetcher.fetchNext());

        when(client.getTasks(any(Tag.class))).then(invocation -> Stream.empty());

        final AsanaObject object = fetcher.fetchNext();
        assertEquals(AsanaObjectState.REMOVED, object.getState());
        assertEquals(task.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getTags();
        verify(client, times(2)).getTasks(project);
        verify(client, times(2)).getTasks(tag);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testCollectMultipleTasksWithSameTagAndFilterOutDuplicates() {
        final Tag anotherTagWithSameName = new Tag();
        anotherTagWithSameName.gid = "555";
        anotherTagWithSameName.name = tag.name;

        when(client.getTags()).then(invocation -> Stream.of(tag, anotherTagWithSameName));

        final Task task1 = new Task();
        task1.gid = "1234";
        task1.name = "My first task";
        task1.modifiedAt = new DateTime(123456789);

        final Task task2 = new Task();
        task2.gid = "1212";
        task2.name = "My other task";
        task2.modifiedAt = new DateTime(234567891);

        final Task task3 = new Task();
        task3.gid = "333";
        task3.name = "My third task";
        task3.modifiedAt = new DateTime(345678912);

        final Task task4 = new Task();
        task4.gid = "444";
        task4.name = "A task without tag";
        task4.modifiedAt = new DateTime(456789123);

        when(client.getTasks(any(Project.class))).then(invocation -> Stream.of(task1, task2, task3, task4));
        when(client.getTasks(tag)).then(invocation -> Stream.of(task1));
        when(client.getTasks(anotherTagWithSameName)).then(invocation -> Stream.of(task1, task2, task3));

        final AsanaObjectFetcher fetcher = new AsanaTaskFetcher(client, project.name, null, tag.name);

        final AsanaObject object1 = fetcher.fetchNext();
        assertEquals(AsanaObjectState.NEW, object1.getState());

        final AsanaObject object2 = fetcher.fetchNext();
        assertEquals(AsanaObjectState.NEW, object2.getState());
        assertNotEquals(object1, object2);

        final AsanaObject object3 = fetcher.fetchNext();
        assertEquals(AsanaObjectState.NEW, object3.getState());
        assertNotEquals(object1, object3);
        assertNotEquals(object2, object3);

        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getTags();
        verify(client, times(1)).getTasks(project);
        verify(client, times(1)).getTasks(tag);
        verify(client, times(1)).getTasks(anotherTagWithSameName);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testTaskUpdatedOnlyWhenModificationDateChanges() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        when(client.getTasks(any(Project.class))).then(invocation -> Stream.of(task));

        final AsanaObjectFetcher fetcher = new AsanaTaskFetcher(client, project.name, null, null);
        assertNotNull(fetcher.fetchNext());
        assertNull(fetcher.fetchNext());

        task.name = "Update my task";
        assertNull(fetcher.fetchNext());

        task.modifiedAt = new DateTime(234567891);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(task.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(3)).getTasks(project);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testRestoreStateAndContinue() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        when(client.getTasks(any(Project.class))).then(invocation -> Stream.of(task));

        final AsanaObjectFetcher fetcher1 = new AsanaTaskFetcher(client, project.name, null, null);
        assertNotNull(fetcher1.fetchNext());

        final AsanaObjectFetcher fetcher2 = new AsanaTaskFetcher(client, project.name, null, null);
        fetcher2.loadState(fetcher1.saveState());

        task.modifiedAt = new DateTime(234567891);
        final AsanaObject object = fetcher2.fetchNext();

        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(task.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(2)).getTasks(project);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testClearState() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        when(client.getTasks(any(Project.class))).then(invocation -> Stream.of(task));

        final AsanaObjectFetcher fetcher = new AsanaTaskFetcher(client, project.name, null, null);
        assertNotNull(fetcher.fetchNext());

        fetcher.clearState();

        task.modifiedAt = new DateTime(234567891);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(task.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(2)).getTasks(project);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testWrongStateForConfigurationThrows() {
        final Project otherProject = new Project();
        otherProject.gid = "999";
        otherProject.name = "Other Project";

        final Section otherSection = new Section();
        otherSection.gid = "888";
        otherSection.name = "Other Section";

        final Tag otherTag = new Tag();
        otherTag.gid = "777";
        otherTag.name = "Other Tag";

        when(client.getProjectByName(otherProject.name)).thenReturn(otherProject);
        when(client.getSectionByName(project, otherSection.name)).thenReturn(otherSection);

        final AsanaObjectFetcher fetcher1 = new AsanaTaskFetcher(client, project.name, null, null);
        final AsanaObjectFetcher fetcher2 = new AsanaTaskFetcher(client, otherProject.name, null, null);
        assertThrows(AsanaObjectFetcherException.class, () -> fetcher2.loadState(fetcher1.saveState()));

        final AsanaObjectFetcher fetcher3 = new AsanaTaskFetcher(client, project.name, section.name, null);
        assertThrows(AsanaObjectFetcherException.class, () -> fetcher3.loadState(fetcher1.saveState()));

        final AsanaObjectFetcher fetcher4 = new AsanaTaskFetcher(client, project.name, null, tag.name);
        assertThrows(AsanaObjectFetcherException.class, () -> fetcher4.loadState(fetcher1.saveState()));

        final AsanaObjectFetcher fetcher5 = new AsanaTaskFetcher(client, project.name, section.name, tag.name);
        assertThrows(AsanaObjectFetcherException.class, () -> fetcher5.loadState(fetcher1.saveState()));

        final AsanaObjectFetcher fetcher6 = new AsanaTaskFetcher(client, project.name, otherSection.name, null);
        assertThrows(AsanaObjectFetcherException.class, () -> fetcher6.loadState(fetcher3.saveState()));

        final AsanaObjectFetcher fetcher7 = new AsanaTaskFetcher(client, project.name, section.name, otherTag.name);
        assertThrows(AsanaObjectFetcherException.class, () -> fetcher7.loadState(fetcher5.saveState()));
    }
}
