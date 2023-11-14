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

import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.model.Attachment;
import org.apache.http.entity.ContentType;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;

import javax.json.Json;
import javax.json.stream.JsonParsingException;
import java.io.StringReader;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.processors.slack.PublishSlack.nullify;
import static org.apache.nifi.util.StringUtils.isEmpty;

public class PostMessageComponent {

    public void addMessageAttachments(ComponentLog logger, PublishConfig config, Set<PropertyDescriptor> properties) {
        // Build Attachments for the posted message and add them to the config's list
        ProcessContext context = config.getContext();
        FlowFile flowFile = config.getFlowFile();
        List<Attachment> attachments = config.getAttachments();
        for (PropertyDescriptor attachmentProperty : properties) {
            String propertyValue = context.getProperty(attachmentProperty).evaluateAttributeExpressions(flowFile).getValue();
            if (propertyValue != null && !propertyValue.isEmpty()) {
                try {
                    Attachment.AttachmentBuilder builder = Attachment.builder()
                            .text(Json.createReader(new StringReader(propertyValue)).readObject().toString())
                            .fallback(propertyValue)    // client logs warnings if fallback isn't set
                            .mimetype(ContentType.APPLICATION_JSON.toString());
                    attachments.add(builder.build());
                } catch (JsonParsingException e) {
                    logger.warn(attachmentProperty.getName() + " property contains invalid JSON, has been skipped.");
                }
            } else {
                logger.warn(attachmentProperty.getName() + " property has no value, has been skipped.");
            }
        }
    }

    public ChatPostMessageRequest buildPostMessageRequest(String message, PublishConfig config) {
        List<Attachment> attachments = config.getAttachments();
        if (isEmpty(message) && attachments.isEmpty()) {
            throw new RuntimeException("The text of the message must be specified if no attachment has been specified and 'Upload File' has been set to 'No'.");
        }
        ChatPostMessageRequest.ChatPostMessageRequestBuilder builder = ChatPostMessageRequest.builder()
                .token(config.getBotToken())
                .channel(config.getChannel())
                .threadTs(nullify(config.getThreadTs()))
                .text(nullify(message))
                .iconUrl(nullify(config.getIconUrl()))
                .iconEmoji(nullify(config.getIconEmoji()))
                .attachments(attachments.isEmpty() ? null : attachments);
        return builder.build();
    }

}
