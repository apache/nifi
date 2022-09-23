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
package org.apache.nifi.controller.asana;

import com.asana.Client;
import com.asana.errors.InvalidTokenError;
import com.asana.models.Attachment;
import com.asana.models.Event;
import com.asana.models.Project;
import com.asana.models.ProjectMembership;
import com.asana.models.ProjectStatus;
import com.asana.models.Resource;
import com.asana.models.ResultBodyCollection;
import com.asana.models.Section;
import com.asana.models.Story;
import com.asana.models.Tag;
import com.asana.models.Task;
import com.asana.models.Team;
import com.asana.models.User;
import com.asana.models.Workspace;
import com.asana.requests.CollectionRequest;
import com.asana.requests.EventsRequest;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class AsanaClientImpl implements AsanaClient {

    static final String ASANA_CLIENT_OPTION_BASE_URL = "base_url";

    private final Client client;
    private final Workspace workspace;

    public AsanaClientImpl(String personalAccessToken, String workspaceName, String baseUrl) {
        client = Client.accessToken(personalAccessToken);
        if (baseUrl != null) {
            client.options.put(ASANA_CLIENT_OPTION_BASE_URL, baseUrl);
        }
        workspace = getWorkspaceByName(workspaceName);
    }

    @Override
    public Project getProjectByName(String projectName) {
        return getProjects()
                .values()
                .stream()
                .filter(p -> p.name.equals(projectName))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No such project: " + projectName));
    }

    @Override
    public Map<String, Project> getProjects() {
        try {
            return collectionRequestToMap(
                    client.projects.getProjects(null, null, workspace.gid, null, null, getSerializedFieldNames(Project.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, User> getUsers() {
        try {
            return collectionRequestToMap(
                    client.users.getUsersForWorkspace(workspace.gid, null, getSerializedFieldNames(User.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<String, ProjectMembership> getProjectMemberships(Project project) {
        try {
            return collectionRequestToMap(
                    client.projectMemberships.getProjectMembershipsForProject(project.gid, null, null, null, getSerializedFieldNames(ProjectMembership.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Team getTeamByName(String teamName) {
        return getTeams()
                .values()
                .stream()
                .filter(t -> t.name.equals(teamName))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No such team: " + teamName));
    }

    @Override
    public Map<String, Team> getTeams() {
        try {
            return collectionRequestToMap(
                    client.teams.getTeamsForWorkspace(workspace.gid, null, null, getSerializedFieldNames(Team.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, User> getTeamMembers(Team team) {
        try {
            return collectionRequestToMap(
                    client.users.getUsersForTeam(team.gid, null, getSerializedFieldNames(User.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Section getSectionByName(Project project, String sectionName) {
        return getSections(project)
                .values()
                .stream()
                .filter(s -> s.name.equals(sectionName))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No such section: " + sectionName + " in project: " + project.name));
    }

    @Override
    public Map<String, Section> getSections(Project project) {
        try {
            return collectionRequestToMap(
                    client.sections.getSectionsForProject(project.gid, null, null, getSerializedFieldNames(Section.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Task> getTasks(Project project) {
        try {
            return collectionRequestToMap(
                    client.tasks.getTasksForProject(project.gid, null, null, null, getSerializedFieldNames(Task.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Task> getTasks(Tag tag) {
        try {
            return collectionRequestToMap(
                    client.tasks.getTasksForTag(tag.gid, null, null, getSerializedFieldNames(Task.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Task> getTasks(Section section) {
        try {
            return collectionRequestToMap(
                    client.tasks.getTasksForSection(section.gid, null, null, getSerializedFieldNames(Task.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Tag> getTags() {
        try {
            return collectionRequestToMap(
                    client.tags.getTags(workspace.gid, null, null, getSerializedFieldNames(Tag.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, ProjectStatus> getProjectStatusUpdates(Project project) {
        try {
            return collectionRequestToMap(
                    client.projectStatuses.getProjectStatusesForProject(project.gid, null, null, getSerializedFieldNames(ProjectStatus.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Story> getStories(Task task) {
        try {
            return collectionRequestToMap(
                    client.stories.getStoriesForTask(task.gid, null, null, getSerializedFieldNames(Story.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Attachment> getAttachments(Task task) {
        try {
            return collectionRequestToMap(
                    client.attachments.getAttachmentsForObject(task.gid, null, null, getSerializedFieldNames(Attachment.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Map<String, Attachment> getAttachments(ProjectStatus projectStatus) {
        try {
            return collectionRequestToMap(
                    client.attachments.getAttachmentsForObject(projectStatus.gid, null, null, getSerializedFieldNames(Attachment.class), false)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Workspace getWorkspaceByName(String workspaceName) {
        List<Workspace> results;
        try {
            results = collectionRequestToMap(client.workspaces.getWorkspaces(null, null, getSerializedFieldNames(Workspace.class), false))
                    .values()
                    .stream()
                    .filter(w -> w.name.equals(workspaceName))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (results.isEmpty()) {
            throw new RuntimeException("No such workspace: " + workspaceName);
        } else if (results.size() > 1) {
            throw new RuntimeException("Multiple workspaces match: " + workspaceName);
        }
        return results.get(0);
    }

    @Override
    public AsanaEventsCollection getEvents(Project project, String syncToken) {
        try {
            String resultSyncToken;
            List<Event> resultEvents = new ArrayList<>();
            try {
                EventsRequest<Event> request = client.events.get(project.gid, syncToken);
                ResultBodyCollection<Event> result = request.executeRaw();

                resultSyncToken = result.sync;
                resultEvents = result.data;
            } catch (InvalidTokenError e) {
                resultSyncToken = e.sync;
            }

            return new AsanaEventsCollection(resultSyncToken, resultEvents);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> List<String> getSerializedFieldNames(Class<T> cls) {
        List<String> result = new ArrayList<>();
        for (Field field : cls.getFields()) {
            SerializedName serializedName = field.getAnnotation(SerializedName.class);
            if (serializedName != null) {
                result.add(serializedName.value());
            } else {
                result.add(field.getName());
            }
        }
        return result;
    }

    private static <T extends Resource> Map<String, T> collectionRequestToMap(CollectionRequest<T> asanaCollectionRequest) {
        return StreamSupport.stream(asanaCollectionRequest.spliterator(), false)
            .collect(Collectors.toMap(item -> item.gid, item -> item));
    }
}
