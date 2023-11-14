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

import com.slack.api.model.Attachment;
import com.slack.api.model.File;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import java.util.ArrayList;
import java.util.List;

public class PublishConfig {
    public ProcessContext getContext() {
        return context;
    }

    public void setContext(ProcessContext context) {
        this.context = context;
    }

    public ProcessSession getSession() {
        return session;
    }

    public void setSession(ProcessSession session) {
        this.session = session;
    }

    public FlowFile getFlowFile() {
        return flowFile;
    }

    public void setFlowFile(FlowFile flowFile) {
        this.flowFile = flowFile;
    }

    public PublishSlackClient getSlackClient() {
        return slackClient;
    }

    public void setSlackClient(PublishSlackClient slackClient) {
        this.slackClient = slackClient;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getBotToken() {
        return botToken;
    }

    public void setBotToken(String botToken) {
        this.botToken = botToken;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getUploadChannel() {
        return uploadChannel;
    }

    public void setUploadChannel(String uploadChannel) {
        this.uploadChannel = uploadChannel;
    }

    public String getThreadTs() {
        return threadTs;
    }

    public void setThreadTs(String threadTs) {
        this.threadTs = threadTs;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public String getInitComment() {
        return initComment;
    }

    public void setInitComment(String initComment) {
        this.initComment = initComment;
    }

    public String getIconUrl() {
        return iconUrl;
    }

    public void setIconUrl(String iconUrl) {
        this.iconUrl = iconUrl;
    }

    public String getIconEmoji() {
        return iconEmoji;
    }

    public void setIconEmoji(String iconEmoji) {
        this.iconEmoji = iconEmoji;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileTitle() {
        return fileTitle;
    }

    public void setFileTitle(String fileTitle) {
        this.fileTitle = fileTitle;
    }

    public byte[] getFileContent() {
        return fileContent;
    }

    public void setFileContent(byte[] fileContent) {
        this.fileContent = fileContent;
    }

    public File getUpload() {
        return upload;
    }

    public void setUpload(File upload) {
        this.upload = upload;
    }

    public List<Attachment> getAttachments() {
        return attachments;
    }

    private ProcessContext context;
    private ProcessSession session;
    private FlowFile flowFile;
    private PublishSlackClient slackClient;
    private String mode;
    private String botToken;
    private String channel;
    private String uploadChannel;
    private String threadTs;
    private String text;
    private String initComment;
    private String iconUrl;
    private String iconEmoji;
    private String fileName;
    private String fileTitle;
    private byte[] fileContent = null;
    private File upload = null;
    private final List<Attachment> attachments = new ArrayList<>();
}
