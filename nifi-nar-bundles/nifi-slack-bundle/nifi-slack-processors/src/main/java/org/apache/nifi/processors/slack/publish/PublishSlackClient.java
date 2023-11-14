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
import com.slack.api.methods.SlackFilesUploadV2Exception;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.request.conversations.ConversationsListRequest;
import com.slack.api.methods.request.files.FilesUploadV2Request;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.conversations.ConversationsListResponse;
import com.slack.api.methods.response.files.FilesUploadV2Response;

import java.io.IOException;


/**
 * A simple wrapper around the Slack Client with the methods that we use so that they can be easily mocked for testing
 */
public interface PublishSlackClient {

    ConversationsListResponse conversationsList(ConversationsListRequest request)
            throws SlackApiException, IOException;

    FilesUploadV2Response filesUploadV2(FilesUploadV2Request request)
            throws SlackFilesUploadV2Exception, SlackApiException, IOException;

    ChatPostMessageResponse chatPostMessage(ChatPostMessageRequest request)
            throws SlackApiException, IOException;

}
