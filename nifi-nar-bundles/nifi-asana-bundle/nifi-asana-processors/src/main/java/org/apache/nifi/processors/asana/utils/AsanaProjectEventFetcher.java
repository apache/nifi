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

import static java.lang.String.format;

import com.asana.Json;
import com.asana.models.Project;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.StreamSupport;
import org.apache.nifi.controller.asana.AsanaClient;
import org.apache.nifi.controller.asana.AsanaEventsCollection;
import org.apache.nifi.util.StringUtils;

public class AsanaProjectEventFetcher extends AbstractAsanaObjectFetcher {

    private static final String PROJECT_GID = ".project.gid";
    private static final String NEXT_SYNC_TOKEN = ".nextSyncToken";

    private final AsanaClient client;
    private final Project project;
    private String nextSyncToken;

    public AsanaProjectEventFetcher(AsanaClient client, String projectName) {
        super();
        this.client = client;
        this.project = client.getProjectByName(projectName);
        this.nextSyncToken = StringUtils.EMPTY;
    }

    @Override
    public Map<String, String> saveState() {
        Map<String, String> state = new HashMap<>();
        state.put(this.getClass().getName() + PROJECT_GID, project.gid);
        state.put(this.getClass().getName() + NEXT_SYNC_TOKEN, nextSyncToken);
        return state;
    }

    @Override
    public void loadState(Map<String, String> state) {
        if (!project.gid.equals(state.get(this.getClass().getName() + PROJECT_GID))) {
            throw new AsanaObjectFetcherException("Project gid does not match.");
        }
        nextSyncToken = state.getOrDefault(this.getClass().getName() + NEXT_SYNC_TOKEN, StringUtils.EMPTY);
    }

    @Override
    public void clearState() {
        nextSyncToken = StringUtils.EMPTY;
    }

    @Override
    protected Iterator<AsanaObject> fetch() {
        AsanaEventsCollection events = client.getEvents(project, nextSyncToken);
        Iterator<AsanaObject> result = StreamSupport.stream(events.spliterator(), false)
            .map(e -> new AsanaObject(
                AsanaObjectState.NEW,
                format("%s.%s.%s.%d", e.type, e.resource.resourceType, e.resource.gid, e.createdAt.getValue()),
                Json.getInstance().toJson(e)))
            .iterator();
        nextSyncToken = events.getNextSyncToken();
        return result;
    }
}
