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
package org.apache.nifi.processors.asana.utils;

import com.asana.models.Project;
import com.asana.models.Section;
import com.asana.models.Task;
import org.apache.nifi.controller.asana.AsanaClient;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class AsanaTaskFetcher extends GenericAsanaObjectFetcher<Task> {
    private static final String SETTINGS_FINGERPRINT = ".settings.fingerprint";

    private final AsanaClient client;
    private final Project project;
    private final Section section;
    private final String tagName;

    public AsanaTaskFetcher(AsanaClient client, String projectName, String sectionName, String tagName) {
        super();
        this.client = client;
        this.project = client.getProjectByName(projectName);

        this.section = Optional.ofNullable(sectionName)
            .map(name -> client.getSectionByName(project, name))
            .orElse(null);

        this.tagName = tagName;
    }

    @Override
    public Map<String, String> saveState() {
        Map<String, String> state = new HashMap<>(super.saveState());
        state.put(this.getClass().getName() + SETTINGS_FINGERPRINT, createSettingsFingerprint());
        return state;
    }

    @Override
    public void loadState(Map<String, String> state) {
        if (!createSettingsFingerprint().equals(state.get(this.getClass().getName() + SETTINGS_FINGERPRINT))) {
            throw new AsanaObjectFetcherException("Settings mismatch.");
        }
        super.loadState(state);
    }

    public Map<String, Task> fetchTasks() {
        Map<String, Task> result;
        if (section != null) {
            result = client.getTasks(section);
        } else {
            result = client.getTasks(project);
        }

        if (tagName != null) {
            Set<String> taskIdsWithTag = client.getTags()
                .values()
                .stream()
                .filter(tag -> tag.name.equals(tagName))
                .map(client::getTasks)
                .flatMap(t -> t.keySet().stream())
                .collect(Collectors.toSet());

            taskIdsWithTag.retainAll(result.keySet());

            return taskIdsWithTag.stream()
                .map(result::get)
                .collect(Collectors.toMap(task -> task.gid, task -> task));
        }

        return result;
    }

    @Override
    protected Map<String, Task> refreshObjects() {
        return fetchTasks();
    }

    @Override
    protected String createObjectFingerprint(Task object) {
        return Long.toString(object.modifiedAt.getValue());
    }

    private String createSettingsFingerprint() {
        return String.join(":", Arrays.asList(
            Optional.ofNullable(project).map(p -> p.gid).orElse(""),
            Optional.ofNullable(section).map(s -> s.gid).orElse(""),
            Optional.ofNullable(tagName).orElse("")
        ));
    }
}
