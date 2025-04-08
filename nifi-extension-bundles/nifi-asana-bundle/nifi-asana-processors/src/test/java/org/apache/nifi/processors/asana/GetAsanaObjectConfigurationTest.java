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
import com.asana.models.ProjectStatus;
import com.asana.models.Tag;
import com.asana.models.Task;
import com.asana.models.Team;
import com.asana.models.User;
import com.google.api.client.util.DateTime;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.asana.AsanaClientProviderService;
import org.apache.nifi.controller.asana.AsanaEventsCollection;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.processors.asana.mocks.MockAsanaClientProviderService;
import org.apache.nifi.processors.asana.mocks.MockDistributedMapCacheClient;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_PROJECTS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_PROJECT_EVENTS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_PROJECT_MEMBERS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_PROJECT_STATUS_ATTACHMENTS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_PROJECT_STATUS_UPDATES;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_STORIES;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_TAGS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_TASKS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_TASK_ATTACHMENTS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_TEAMS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_TEAM_MEMBERS;
import static org.apache.nifi.processors.asana.AsanaObjectType.AV_COLLECT_USERS;
import static org.apache.nifi.processors.asana.GetAsanaObject.PROP_ASANA_CLIENT_SERVICE;
import static org.apache.nifi.processors.asana.GetAsanaObject.PROP_ASANA_OBJECT_TYPE;
import static org.apache.nifi.processors.asana.GetAsanaObject.PROP_ASANA_OUTPUT_BATCH_SIZE;
import static org.apache.nifi.processors.asana.GetAsanaObject.PROP_ASANA_PROJECT;
import static org.apache.nifi.processors.asana.GetAsanaObject.PROP_ASANA_TEAM_NAME;
import static org.apache.nifi.processors.asana.GetAsanaObject.PROP_DISTRIBUTED_CACHE_SERVICE;
import static org.apache.nifi.processors.asana.GetAsanaObject.REL_NEW;
import static org.apache.nifi.util.TestRunners.newTestRunner;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class GetAsanaObjectConfigurationTest {

    private TestRunner runner;
    private MockAsanaClientProviderService mockService;
    private MockDistributedMapCacheClient mockDistributedMapCacheClient;

    @BeforeEach
    public void init() {
        runner = newTestRunner(GetAsanaObject.class);
        mockService = new MockAsanaClientProviderService();
        mockDistributedMapCacheClient = new MockDistributedMapCacheClient();
    }

    @Test
    public void testNotValidWithoutControllerService() {
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);
        runner.assertNotValid();
    }

    @Test
    public void testNotValidWithoutDistributedMapCacheClient() throws InitializationException {
        withMockAsanaClientService();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);
        runner.assertNotValid();
    }

    @Test
    public void testBatchSizeMustBePositiveInteger() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);

        runner.setProperty(PROP_ASANA_OUTPUT_BATCH_SIZE, StringUtils.EMPTY);
        runner.assertNotValid();

        runner.setProperty(PROP_ASANA_OUTPUT_BATCH_SIZE, "Lorem");
        runner.assertNotValid();

        runner.setProperty(PROP_ASANA_OUTPUT_BATCH_SIZE, "-1");
        runner.assertNotValid();

        runner.setProperty(PROP_ASANA_OUTPUT_BATCH_SIZE, "0");
        runner.assertNotValid();

        runner.setProperty(PROP_ASANA_OUTPUT_BATCH_SIZE, "100");
        runner.assertValid();
    }

    @Test
    public void testValidConfigurations() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);
        runner.assertValid();

        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TASKS);
        runner.setProperty(PROP_ASANA_PROJECT, "My Project");
        runner.assertValid();

        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TEAM_MEMBERS);
        runner.setProperty(PROP_ASANA_TEAM_NAME, "A team");
        runner.assertValid();
    }

    @Test
    public void testConfigurationInvalidWithoutProjectName() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TASKS);
        runner.assertNotValid();

        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECT_MEMBERS);
        runner.assertNotValid();

        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_STORIES);
        runner.assertNotValid();

        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECT_EVENTS);
        runner.assertNotValid();

        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECT_STATUS_ATTACHMENTS);
        runner.assertNotValid();

        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECT_STATUS_UPDATES);
        runner.assertNotValid();

        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TASK_ATTACHMENTS);
        runner.assertNotValid();
    }

    @Test
    public void testConfigurationInvalidWithoutTeamName() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TEAM_MEMBERS);
    }

    @Test
    public void testCollectProjects() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECTS);

        final Project project = new Project();
        project.gid = "12345";
        project.modifiedAt = new DateTime(123456789);

        when(mockService.client.getProjects()).then(invocation -> Stream.of(project));

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 1);
        verify(mockService.client, atLeastOnce()).getProjects();
        verifyNoMoreInteractions(mockService.client);
    }

    @Test
    public void testCollectTeams() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TEAMS);

        final Team team = new Team();
        team.gid = "12345";

        when(mockService.client.getTeams()).then(invocation -> Stream.of(team));

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 1);
        verify(mockService.client, atLeastOnce()).getTeams();
        verifyNoMoreInteractions(mockService.client);
    }

    @Test
    public void testCollectTeamMembers() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TEAM_MEMBERS);
        runner.setProperty(PROP_ASANA_TEAM_NAME, "A team");

        final Team team = new Team();
        team.gid = "12345";

        when(mockService.client.getTeamByName("A team")).thenReturn(team);
        when(mockService.client.getTeamMembers(any())).then(invocation -> Stream.empty());

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 0);
        verify(mockService.client, atLeastOnce()).getTeamByName("A team");
        verify(mockService.client, atLeastOnce()).getTeamMembers(any());
        verifyNoMoreInteractions(mockService.client);
    }

    @Test
    public void testCollectUsers() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_USERS);

        final User user = new User();
        user.gid = "12345";

        when(mockService.client.getUsers()).then(invocation -> Stream.of(user));

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 1);
        verify(mockService.client, atLeastOnce()).getUsers();
        verifyNoMoreInteractions(mockService.client);
    }

    @Test
    public void testCollectTags() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TAGS);

        final Tag tag = new Tag();
        tag.gid = "12345";

        when(mockService.client.getTags()).then(invocation -> Stream.of(tag));

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 1);
        verify(mockService.client, atLeastOnce()).getTags();
        verifyNoMoreInteractions(mockService.client);
    }

    @Test
    public void testCollectProjectEvents() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECT_EVENTS);
        runner.setProperty(PROP_ASANA_PROJECT, "My Project");

        final Project project = new Project();
        project.gid = "12345";
        project.modifiedAt = new DateTime(123456789);

        final AsanaEventsCollection events = new AsanaEventsCollection("foo", emptyList());

        when(mockService.client.getProjectByName("My Project")).thenReturn(project);
        when(mockService.client.getEvents(any(), any())).thenReturn(events);

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 0);
        verify(mockService.client, atLeastOnce()).getProjectByName("My Project");
        verify(mockService.client, atLeastOnce()).getEvents(any(), any());
        verifyNoMoreInteractions(mockService.client);
    }

    @Test
    public void testCollectProjectMembers() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECT_MEMBERS);
        runner.setProperty(PROP_ASANA_PROJECT, "My Project");

        final Project project = new Project();
        project.gid = "12345";
        project.modifiedAt = new DateTime(123456789);

        when(mockService.client.getProjectByName("My Project")).thenReturn(project);
        when(mockService.client.getProjectMemberships(any())).then(invocation -> Stream.empty());

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 0);
        verify(mockService.client, atLeastOnce()).getProjectByName("My Project");
        verify(mockService.client, atLeastOnce()).getProjectMemberships(any());
        verifyNoMoreInteractions(mockService.client);
    }

    @Test
    public void testCollectProjectStatusUpdates() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECT_STATUS_UPDATES);
        runner.setProperty(PROP_ASANA_PROJECT, "My Project");

        final Project project = new Project();
        project.gid = "12345";
        project.modifiedAt = new DateTime(123456789);

        when(mockService.client.getProjectByName("My Project")).thenReturn(project);
        when(mockService.client.getProjectStatusUpdates(any())).then(invocation -> Stream.empty());

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 0);
        verify(mockService.client, atLeastOnce()).getProjectByName("My Project");
        verify(mockService.client, atLeastOnce()).getProjectStatusUpdates(any());
        verifyNoMoreInteractions(mockService.client);
    }

    @Test
    public void testCollectProjectStatusAttachments() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_PROJECT_STATUS_ATTACHMENTS);
        runner.setProperty(PROP_ASANA_PROJECT, "My Project");

        final Project project = new Project();
        project.gid = "12345";
        project.modifiedAt = new DateTime(123456789);

        final ProjectStatus projectStatus = new ProjectStatus();
        projectStatus.gid = "12345";

        when(mockService.client.getProjectByName("My Project")).thenReturn(project);
        when(mockService.client.getProjectStatusUpdates(any())).then(invocation -> Stream.of(projectStatus));
        when(mockService.client.getAttachments(any(ProjectStatus.class))).then(invocation -> Stream.empty());

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 0);
        verify(mockService.client, atLeastOnce()).getProjectByName("My Project");
        verify(mockService.client, atLeastOnce()).getProjectStatusUpdates(any());
        verify(mockService.client, atLeastOnce()).getAttachments(any(ProjectStatus.class));
        verifyNoMoreInteractions(mockService.client);
    }

    @Test
    public void testCollectTasks() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TASKS);
        runner.setProperty(PROP_ASANA_PROJECT, "My Project");

        final Project project = new Project();
        project.gid = "12345";
        project.modifiedAt = new DateTime(123456789);

        final Task task = new Task();
        task.gid = "12345";
        task.modifiedAt = new DateTime(123456789);

        when(mockService.client.getProjectByName("My Project")).thenReturn(project);
        when(mockService.client.getTasks(any(Project.class))).then(invocation -> Stream.of(task));

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 1);
        verify(mockService.client, atLeastOnce()).getProjectByName("My Project");
        verify(mockService.client, atLeastOnce()).getTasks(any(Project.class));
        verifyNoMoreInteractions(mockService.client);
    }

    @Test
    public void testCollectTaskAttachments() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_TASK_ATTACHMENTS);
        runner.setProperty(PROP_ASANA_PROJECT, "My Project");

        final Project project = new Project();
        project.gid = "12345";
        project.modifiedAt = new DateTime(123456789);

        final Task task = new Task();
        task.gid = "12345";
        task.modifiedAt = new DateTime(123456789);

        when(mockService.client.getProjectByName("My Project")).thenReturn(project);
        when(mockService.client.getTasks(any(Project.class))).then(invocation -> Stream.of(task));
        when(mockService.client.getAttachments(any(Task.class))).then(invocation -> Stream.empty());

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 0);
        verify(mockService.client, atLeastOnce()).getProjectByName("My Project");
        verify(mockService.client, atLeastOnce()).getTasks(any(Project.class));
        verify(mockService.client, atLeastOnce()).getAttachments(any(Task.class));
        verifyNoMoreInteractions(mockService.client);
    }

    @Test
    public void testCollectStories() throws InitializationException {
        withMockAsanaClientService();
        withMockDistributedMapCacheClient();
        runner.setProperty(PROP_ASANA_OBJECT_TYPE, AV_COLLECT_STORIES);
        runner.setProperty(PROP_ASANA_PROJECT, "My Project");

        final Project project = new Project();
        project.gid = "12345";
        project.modifiedAt = new DateTime(123456789);

        final Task task = new Task();
        task.gid = "12345";
        task.modifiedAt = new DateTime(123456789);

        when(mockService.client.getProjectByName("My Project")).thenReturn(project);
        when(mockService.client.getTasks(any(Project.class))).then(invocation -> Stream.of(task));
        when(mockService.client.getStories(any())).then(invocation -> Stream.empty());

        runner.run(1);

        runner.assertTransferCount(REL_NEW, 0);
        verify(mockService.client, atLeastOnce()).getProjectByName("My Project");
        verify(mockService.client, atLeastOnce()).getTasks(any(Project.class));
        verify(mockService.client, atLeastOnce()).getStories(any());
        verifyNoMoreInteractions(mockService.client);
    }

    private void withMockAsanaClientService() throws InitializationException {
        final String serviceIdentifier = AsanaClientProviderService.class.getName();
        runner.addControllerService(serviceIdentifier, mockService);
        runner.enableControllerService(mockService);
        runner.setProperty(PROP_ASANA_CLIENT_SERVICE, serviceIdentifier);
    }

    private void withMockDistributedMapCacheClient() throws InitializationException {
        final String serviceIdentifier = DistributedMapCacheClient.class.getName();
        runner.addControllerService(serviceIdentifier, mockDistributedMapCacheClient);
        runner.enableControllerService(mockDistributedMapCacheClient);
        runner.setProperty(PROP_DISTRIBUTED_CACHE_SERVICE, serviceIdentifier);
    }

}
