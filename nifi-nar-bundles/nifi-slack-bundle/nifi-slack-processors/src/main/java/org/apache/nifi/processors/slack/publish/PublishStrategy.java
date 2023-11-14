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
package org.apache.nifi.processors.slack.publish;

import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.request.files.FilesUploadV2Request;
import com.slack.api.model.File;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;
import java.util.Set;

public interface PublishStrategy {
    boolean willSendContent();
    boolean willUploadFile();
    boolean willPostMessage();
    void setFileContent(PublishConfig config, byte[] fileData);
    void setUploadFile(PublishConfig config, File uploadFile);

    FilesUploadV2Request buildFilesUploadV2Request(ComponentLog logger, PublishConfig config) throws SlackApiException, IOException;

    void addMessageAttachments(ComponentLog logger, PublishConfig config, Set<PropertyDescriptor> properties);

    ChatPostMessageRequest buildPostMessageRequest(PublishConfig config);
}
