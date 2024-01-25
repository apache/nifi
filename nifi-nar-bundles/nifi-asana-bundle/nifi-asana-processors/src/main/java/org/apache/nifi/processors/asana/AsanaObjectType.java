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

import org.apache.nifi.components.DescribedValue;

public enum AsanaObjectType implements DescribedValue {
    AV_COLLECT_TASKS(
            "asana-collect-tasks",
            "Tasks",
            "Collect tasks matching to the specified conditions."
    ),
    AV_COLLECT_TASK_ATTACHMENTS(
            "asana-collect-task-attachments",
            "Task Attachments",
            "Collect attached files of tasks matching to the specified conditions."
    ),
    AV_COLLECT_PROJECTS(
            "asana-collect-projects",
            "Projects",
            "Collect projects of the workspace."
    ),
    AV_COLLECT_TAGS(
            "asana-collect-tags",
            "Tags",
            "Collect tags of the workspace."
    ),
    AV_COLLECT_USERS(
            "asana-collect-users",
            "Users",
            "Collect users assigned to the workspace."
    ),
    AV_COLLECT_PROJECT_MEMBERS(
            "asana-collect-project-members",
            "Members of a Project",
            "Collect users assigned to the specified project."
    ),
    AV_COLLECT_TEAMS(
            "asana-collect-teams",
            "Teams",
            "Collect teams of the workspace."
    ),
    AV_COLLECT_TEAM_MEMBERS(
            "asana-collect-team-members",
            "Team Members",
            "Collect users assigned to the specified team."
    ),
    AV_COLLECT_STORIES(
            "asana-collect-stories",
            "Stories of Tasks",
            "Collect stories (comments) of of tasks matching to the specified conditions."
    ),
    AV_COLLECT_PROJECT_STATUS_UPDATES(
            "asana-collect-project-status-updates",
            "Status Updates of a Project",
            "Collect status updates of the specified project."
    ),
    AV_COLLECT_PROJECT_STATUS_ATTACHMENTS(
            "asana-collect-project-status-attachments",
            "Attachments of Status Updates",
            "Collect attached files of project status updates."
    ),
    AV_COLLECT_PROJECT_EVENTS(
            "asana-collect-project-events",
            "Events of a Project",
            "Collect various events happening on the specified project and on its' tasks."
    );

    private final String value;
    private final String displayName;
    private final String description;

    AsanaObjectType(String value, String displayName, String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
