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
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;

import java.io.IOException;
import java.util.Set;

public abstract class AbstractStrategy implements PublishStrategy {

    private final PostMessageComponent postMessageComponent = new PostMessageComponent();
    private final UploadComponent uploadComponent = new UploadComponent();

    @Override
    public boolean willSendContent() {
        return false;
    }

    @Override
    public boolean willUploadFile() {
        return false;
    }

    @Override
    public boolean willPostMessage() {
        return false;
    }

    @Override
    public void setFileContent(PublishConfig config, byte[] fileContent) {
        config.setFileContent(fileContent);
    }

    @Override
    public void setUploadFile(PublishConfig config, File uploadFile) {
        config.setUpload(uploadFile);
        ProcessSession session = config.getSession();
        FlowFile flowFile = config.getFlowFile();
        if (session != null && flowFile != null && uploadFile.getUrlPrivate() != null) {
            session.putAttribute(flowFile, "slack.file.url", uploadFile.getUrlPrivate());
            session.getProvenanceReporter().send(flowFile, uploadFile.getUrlPrivate());
        }
    }

    protected String getMessageText(PublishConfig config) {
        return config.getText();
    }

    protected String getUploadChannel(PublishConfig config) {
        return config.getUploadChannel();
    }

    protected String getInitialComment(PublishConfig config) {
        return config.getInitComment();
    }

    @Override
    public FilesUploadV2Request buildFilesUploadV2Request(ComponentLog logger, PublishConfig config) throws SlackApiException, IOException {
        return uploadComponent.buildFilesUploadV2Request(logger, getUploadChannel(config), getInitialComment(config), config);
    }

    @Override
    public void addMessageAttachments(ComponentLog logger, PublishConfig config, Set<PropertyDescriptor> properties) {
        postMessageComponent.addMessageAttachments(logger, config, properties);
    }

    @Override
    public ChatPostMessageRequest buildPostMessageRequest(PublishConfig config) {
        return postMessageComponent.buildPostMessageRequest(getMessageText(config), config);
    }
}
