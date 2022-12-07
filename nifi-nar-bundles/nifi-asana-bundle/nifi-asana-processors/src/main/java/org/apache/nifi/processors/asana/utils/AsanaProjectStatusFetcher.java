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
import com.asana.models.ProjectStatus;
import java.util.stream.Stream;
import org.apache.nifi.controller.asana.AsanaClient;

import java.util.HashMap;
import java.util.Map;

public class AsanaProjectStatusFetcher extends GenericAsanaObjectFetcher<ProjectStatus> {
    private static final String PROJECT_GID = ".project.gid";

    private final AsanaClient client;
    private final Project project;

    public AsanaProjectStatusFetcher(AsanaClient client, String projectName) {
        super();
        this.client = client;
        this.project = client.getProjectByName(projectName);
    }

    @Override
    public Map<String, String> saveState() {
        Map<String, String> state = new HashMap<>(super.saveState());
        state.put(this.getClass().getName() + PROJECT_GID, project.gid);
        return state;
    }

    @Override
    public void loadState(Map<String, String> state) {
        if (!project.gid.equals(state.get(this.getClass().getName() + PROJECT_GID))) {
            throw new AsanaObjectFetcherException("Project gid does not match.");
        }
        super.loadState(state);
    }

    @Override
    protected Stream<ProjectStatus> fetchObjects() {
        return client.getProjectStatusUpdates(project);
    }
}
