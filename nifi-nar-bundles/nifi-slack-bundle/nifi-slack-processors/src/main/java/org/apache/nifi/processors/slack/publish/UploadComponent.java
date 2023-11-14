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
import com.slack.api.methods.request.files.FilesUploadV2Request;
import org.apache.nifi.logging.ComponentLog;

import java.io.IOException;

import static org.apache.nifi.processors.slack.PublishSlack.lookupChannelId;
import static org.apache.nifi.processors.slack.PublishSlack.nullify;
import static org.apache.nifi.util.StringUtils.isEmpty;

public class UploadComponent {

    public FilesUploadV2Request buildFilesUploadV2Request(
            ComponentLog logger,
            String channel,
            String initialComment,
            PublishConfig config) throws SlackApiException, IOException {
        String channelId = isEmpty(channel) ? null : lookupChannelId(config.getSlackClient(), channel);
        String fileName = config.getFileName();
        if (fileName == null || fileName.isEmpty()) {
            fileName = "file";
            logger.warn("File name not specified, has been set to {}.", fileName);
        }
        final String title = config.getFileTitle();
        FilesUploadV2Request.FilesUploadV2RequestBuilder builder = FilesUploadV2Request.builder()
                .token(config.getBotToken())
                .channel(channelId)
                .threadTs(isEmpty(channelId) ? null : nullify(config.getThreadTs()))
                .filename(fileName)
                .title(isEmpty(title) ? fileName : title)
                .fileData(config.getFileContent())
                .initialComment(nullify(initialComment));
        return builder.build();
    }

}
