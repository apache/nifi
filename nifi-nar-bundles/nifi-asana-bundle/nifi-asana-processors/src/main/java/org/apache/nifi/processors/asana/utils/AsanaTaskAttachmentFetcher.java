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

import com.asana.models.Attachment;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.nifi.controller.asana.AsanaClient;

public class AsanaTaskAttachmentFetcher extends GenericAsanaObjectFetcher<Attachment> {
    private final AsanaClient client;
    private final AsanaTaskFetcher taskFetcher;

    public AsanaTaskAttachmentFetcher(AsanaClient client, String projectName, String sectionName, String tagName) {
        super();
        this.client = client;
        this.taskFetcher = new AsanaTaskFetcher(client, projectName, sectionName, tagName);
    }

    @Override
    public Map<String, String> saveState() {
        Map<String, String> state = new HashMap<>();
        state.putAll(taskFetcher.saveState());
        state.putAll(super.saveState());
        return state;
    }

    @Override
    public void loadState(Map<String, String> state) {
        taskFetcher.loadState(state);
        super.loadState(state);
    }

    @Override
    protected Stream<Attachment> fetchObjects() {
        return fetchTaskAttachments();
    }

    @Override
    protected String createObjectFingerprint(Attachment object) {
        return Long.toString(object.createdAt.getValue());
    }

    private Stream<Attachment> fetchTaskAttachments() {
        return taskFetcher.fetchTasks().flatMap(client::getAttachments);
    }
}
