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

import com.asana.models.Project;
import com.asana.models.Section;
import com.asana.models.Story;
import com.asana.models.Tag;
import com.asana.models.Task;
import com.google.api.client.util.DateTime;
import org.apache.nifi.controller.asana.AsanaClient;
import org.apache.nifi.processors.asana.utils.AsanaObject;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcher;
import org.apache.nifi.processors.asana.utils.AsanaObjectFetcherException;
import org.apache.nifi.processors.asana.utils.AsanaObjectState;
import org.apache.nifi.processors.asana.utils.AsanaStoryFetcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
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

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class AsanaStoryFetcherTest {

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

        when(client.getSections(project)).thenReturn(singletonMap(section.gid, section));
        when(client.getSectionByName(project, section.name)).thenReturn(section);

        tag = new Tag();
        tag.gid = "9876";
        tag.name = "Foo";
        tag.createdAt = new DateTime(123456789);

        when(client.getTags()).thenReturn(singletonMap(tag.gid, tag));
    }

    @Test
    public void testNoObjectsFetchedWhenNoStoriesReturned() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        when(client.getTasks(any(Project.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getStories(any(Task.class))).thenReturn(emptyMap());

        final AsanaObjectFetcher fetcher = new AsanaStoryFetcher(client, project.name, null, null);
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(1)).getTasks(project);
        verify(client, times(1)).getStories(task);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testNoObjectsFetchedWhenNoStoriesReturnedBySection() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        when(client.getTasks(any(Section.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getStories(any(Task.class))).thenReturn(emptyMap());

        final AsanaObjectFetcher fetcher = new AsanaStoryFetcher(client, project.name, section.name, null);
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getSectionByName(project, section.name);
        verify(client, times(1)).getTasks(section);
        verify(client, times(1)).getStories(task);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testNoObjectsFetchedWhenNoStoriesReturnedByTag() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        when(client.getTasks(any(Project.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getTasks(any(Tag.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getStories(any(Task.class))).thenReturn(emptyMap());

        final AsanaObjectFetcher fetcher = new AsanaStoryFetcher(client, project.name, null, tag.name);
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getTags();
        verify(client, times(1)).getTasks(project);
        verify(client, times(1)).getTasks(tag);
        verify(client, times(1)).getStories(task);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleStoryFetched() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        final Story story = new Story();
        story.gid = "99";
        story.createdAt = new DateTime(123456789);
        story.text = "Lorem Ipsum";

        when(client.getTasks(any(Project.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getStories(any(Task.class))).thenReturn(singletonMap(story.gid, story));

        final AsanaObjectFetcher fetcher = new AsanaStoryFetcher(client, project.name, null, null);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(story.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(1)).getTasks(project);
        verify(client, times(1)).getStories(task);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleStoryFetchedBySection() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        final Story story = new Story();
        story.gid = "99";
        story.createdAt = new DateTime(123456789);
        story.text = "Lorem Ipsum";

        when(client.getTasks(any(Section.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getStories(any(Task.class))).thenReturn(singletonMap(story.gid, story));

        final AsanaObjectFetcher fetcher = new AsanaStoryFetcher(client, project.name, section.name, null);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(story.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getSectionByName(project, section.name);
        verify(client, times(1)).getTasks(section);
        verify(client, times(1)).getStories(task);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testSingleStoryFetchedByTag() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        final Story story = new Story();
        story.gid = "99";
        story.createdAt = new DateTime(123456789);
        story.text = "Lorem Ipsum";

        when(client.getTasks(any(Project.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getTasks(any(Tag.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getStories(any(Task.class))).thenReturn(singletonMap(story.gid, story));

        final AsanaObjectFetcher fetcher = new AsanaStoryFetcher(client, project.name, null, tag.name);
        final AsanaObject object = fetcher.fetchNext();

        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(story.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getTags();
        verify(client, times(1)).getTasks(project);
        verify(client, times(1)).getTasks(tag);
        verify(client, times(1)).getStories(task);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testNoStoryFetchedByNonMatchingTag() {
        final Task task1 = new Task();
        task1.gid = "1234";
        task1.name = "My first task";
        task1.modifiedAt = new DateTime(123456789);

        final Task task2 = new Task();
        task2.gid = "5678";
        task2.name = "My other task";
        task2.modifiedAt = new DateTime(123456789);

        final Story story1 = new Story();
        story1.gid = "99";
        story1.createdAt = new DateTime(123456789);
        story1.text = "Lorem Ipsum";

        final Story story2 = new Story();
        story2.gid = "88";
        story2.createdAt = new DateTime(123456789);
        story2.text = "My other Lorem Ipsum";

        when(client.getTasks(any(Project.class))).thenReturn(singletonMap(task1.gid, task1));
        when(client.getTasks(any(Tag.class))).thenReturn(singletonMap(task2.gid, task2));
        when(client.getStories(task1)).thenReturn(singletonMap(story1.gid, story1));
        when(client.getStories(task2)).thenReturn(singletonMap(story2.gid, story2));

        final AsanaObjectFetcher fetcher = new AsanaStoryFetcher(client, project.name, null, tag.name);
        assertNull(fetcher.fetchNext());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getTags();
        verify(client, times(1)).getTasks(project);
        verify(client, times(1)).getTasks(tag);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testTaskOfStoryRemovedFromSection() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        final Story story = new Story();
        story.gid = "99";
        story.createdAt = new DateTime(123456789);
        story.text = "Lorem Ipsum";

        when(client.getTasks(any(Section.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getStories(any(Task.class))).thenReturn(singletonMap(story.gid, story));

        final AsanaObjectFetcher fetcher = new AsanaStoryFetcher(client, project.name, section.name, null);
        assertNotNull(fetcher.fetchNext());

        when(client.getTasks(any(Section.class))).thenReturn(emptyMap());

        final AsanaObject object = fetcher.fetchNext();
        assertEquals(AsanaObjectState.REMOVED, object.getState());
        assertEquals(story.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getSectionByName(project, section.name);
        verify(client, times(2)).getTasks(section);
        verify(client, times(1)).getStories(task);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testTaskOfStoryUntagged() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        final Story story = new Story();
        story.gid = "99";
        story.createdAt = new DateTime(123456789);
        story.text = "Lorem Ipsum";

        when(client.getTasks(any(Project.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getTasks(any(Tag.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getStories(any(Task.class))).thenReturn(singletonMap(story.gid, story));

        final AsanaObjectFetcher fetcher = new AsanaStoryFetcher(client, project.name, null, tag.name);
        assertNotNull(fetcher.fetchNext());

        when(client.getTasks(any(Tag.class))).thenReturn(emptyMap());

        final AsanaObject object = fetcher.fetchNext();
        assertEquals(AsanaObjectState.REMOVED, object.getState());
        assertEquals(story.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, atLeastOnce()).getTags();
        verify(client, times(2)).getTasks(project);
        verify(client, times(2)).getTasks(tag);
        verify(client, times(1)).getStories(task);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testStoryUpdatedWhenAnyPartChanges() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        final Story story = new Story();
        story.gid = "99";
        story.createdAt = new DateTime(123456789);
        story.text = "Lorem Ipsum";

        when(client.getTasks(any(Project.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getStories(any(Task.class))).thenReturn(singletonMap(story.gid, story));
        final AsanaObjectFetcher fetcher = new AsanaStoryFetcher(client, project.name, null, null);
        assertNotNull(fetcher.fetchNext());
        assertNull(fetcher.fetchNext());

        story.text = "Bla bla";

        final AsanaObject object = fetcher.fetchNext();
        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(story.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(3)).getTasks(project);
        verify(client, times(3)).getStories(task);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testRestoreStateAndContinue() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        final Story story = new Story();
        story.gid = "99";
        story.createdAt = new DateTime(123456789);
        story.text = "Lorem Ipsum";

        when(client.getTasks(any(Project.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getStories(any(Task.class))).thenReturn(singletonMap(story.gid, story));
        final AsanaObjectFetcher fetcher1 = new AsanaStoryFetcher(client, project.name, null, null);
        assertNotNull(fetcher1.fetchNext());

        final AsanaObjectFetcher fetcher2 = new AsanaStoryFetcher(client, project.name, null, null);
        fetcher2.loadState(fetcher1.saveState());

        story.text = "Bla bla";

        final AsanaObject object = fetcher1.fetchNext();
        assertEquals(AsanaObjectState.UPDATED, object.getState());
        assertEquals(story.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(2)).getTasks(project);
        verify(client, times(2)).getStories(task);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testClearState() {
        final Task task = new Task();
        task.gid = "1234";
        task.name = "My first task";
        task.modifiedAt = new DateTime(123456789);

        final Story story = new Story();
        story.gid = "99";
        story.createdAt = new DateTime(123456789);
        story.text = "Lorem Ipsum";

        when(client.getTasks(any(Project.class))).thenReturn(singletonMap(task.gid, task));
        when(client.getStories(any(Task.class))).thenReturn(singletonMap(story.gid, story));
        final AsanaObjectFetcher fetcher = new AsanaStoryFetcher(client, project.name, null, null);
        assertNotNull(fetcher.fetchNext());

        fetcher.clearState();

        story.text = "Bla bla";

        final AsanaObject object = fetcher.fetchNext();
        assertEquals(AsanaObjectState.NEW, object.getState());
        assertEquals(story.gid, object.getGid());

        verify(client, atLeastOnce()).getProjectByName(project.name);
        verify(client, times(2)).getTasks(project);
        verify(client, times(2)).getStories(task);
        verifyNoMoreInteractions(client);
    }

    @Test
    public void testWrongStateForConfigurationThrows() {
        final Project otherProject = new Project();
        otherProject.gid = "999";
        otherProject.name = "Other Project";

        when(client.getProjectByName(otherProject.name)).thenReturn(otherProject);

        final AsanaObjectFetcher fetcher1 = new AsanaStoryFetcher(client, project.name, null, null);
        final AsanaObjectFetcher fetcher2 = new AsanaStoryFetcher(client, otherProject.name, null, null);
        assertThrows(AsanaObjectFetcherException.class, () -> fetcher2.loadState(fetcher1.saveState()));
    }
}
