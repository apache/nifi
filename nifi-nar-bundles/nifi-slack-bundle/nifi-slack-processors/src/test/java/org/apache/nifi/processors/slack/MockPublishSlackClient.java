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
package org.apache.nifi.processors.slack;

import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.request.chat.ChatPostMessageRequest;
import com.slack.api.methods.request.conversations.ConversationsListRequest;
import com.slack.api.methods.request.files.FilesUploadV2Request;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.methods.response.conversations.ConversationsListResponse;
import com.slack.api.methods.response.files.FilesUploadV2Response;
import com.slack.api.model.Conversation;
import com.slack.api.model.File;
import com.slack.api.model.ResponseMetadata;
import okhttp3.Response;
import org.apache.nifi.processors.slack.publish.PublishSlackClient;

import java.io.IOException;
import java.util.List;

public class MockPublishSlackClient implements PublishSlackClient {

    public static final String VALID_TOKEN = "bot-access-token";
    public static final String INVALID_TOKEN = "invalid-token";
    public static final String VALID_CHANNEL = "my-channel";
    public static final String VALID_CHANNEL_ID = "C0000000000";
    public static final String INVALID_CHANNEL = "wrong-channel";
    public static final String ERROR_CHANNEL = "error-channel";
    public static final String WARNING_CHANNEL = "warning-channel";
    public static final String VALID_TEXT = "my-text";
    public static final String INTERNATIONAL_TEXT = "Iñtërnâtiônàližætiøn";
    public static final String EMPTY_TEXT = "${dummy}";
    public static final String ERROR_TEXT = "slack-error";
    public static final String INTERNAL_ERROR_TEXT = "internal_error";
    public static final String TOKEN_REVOKED_TEXT = "token_revoked";
    public static final String NOT_IN_CHANNEL_TEXT = "not_in_channel";
    public static final String WARNING_TEXT = "Warning - Token Expiration in 5 days!";
    public static final String IO_EXCEPT_TEXT = "io-exception";
    public static final String SLACK_EXCEPT_TEXT = "slack-exception";
    public static final String FLOWFILE_CONTENT = "FlowFile Content - Hello, world!";
    public static final String BASE_PERMA_LINK = "https://files.slack.com/foo/bar/";
    public static final String BASE_URL_PRIVATE = "https://private.slack.com/super/secret/";
    public static final int UPLOAD_FILE_SIZE = 28248282;

    private static final ChatPostMessageResponse CHAT_INVALID_TOKEN_RESPONSE;
    private static final ChatPostMessageResponse CHAT_INVALID_CHANNEL_RESPONSE;
    private static final ConversationsListResponse CONVERSATIONS_NORMAL_RESPONSE;
    private ChatPostMessageRequest lastChatRequest = null;
    private ChatPostMessageResponse lastChatResponse = null;
    private ConversationsListRequest lastConvoRequest = null;
    private ConversationsListResponse lastConvoResponse = null;
    private FilesUploadV2Request lastFileRequest = null;
    private FilesUploadV2Response lastFileResponse = null;

    static {
        CHAT_INVALID_TOKEN_RESPONSE = new ChatPostMessageResponse();
        CHAT_INVALID_TOKEN_RESPONSE.setOk(false);
        CHAT_INVALID_TOKEN_RESPONSE.setError(TOKEN_REVOKED_TEXT);
        CHAT_INVALID_CHANNEL_RESPONSE = new ChatPostMessageResponse();
        CHAT_INVALID_CHANNEL_RESPONSE.setOk(false);
        CHAT_INVALID_CHANNEL_RESPONSE.setError(NOT_IN_CHANNEL_TEXT);
        CONVERSATIONS_NORMAL_RESPONSE = new ConversationsListResponse();
        CONVERSATIONS_NORMAL_RESPONSE.setOk(true);
        Conversation channel = Conversation.builder()
                .isChannel(true)
                .id(VALID_CHANNEL_ID)
                .name(VALID_CHANNEL)
                .build();
        CONVERSATIONS_NORMAL_RESPONSE.setChannels(List.of(channel));
        CONVERSATIONS_NORMAL_RESPONSE.setResponseMetadata(new ResponseMetadata());
    }

    @Override
    public ConversationsListResponse conversationsList(ConversationsListRequest request) throws IOException, SlackApiException {
        lastConvoRequest = request;
        // check request token and cursor to see if we should throw an exception, etc
        lastConvoResponse = CONVERSATIONS_NORMAL_RESPONSE;
        return lastConvoResponse;
    }

    @Override
    public FilesUploadV2Response filesUploadV2(FilesUploadV2Request request) throws IOException, SlackApiException {
        lastFileRequest = request;
        if (IO_EXCEPT_TEXT.equals(request.getInitialComment())) {
            throw new IOException(IO_EXCEPT_TEXT);
        }
        if (SLACK_EXCEPT_TEXT.equals(request.getInitialComment())) {
            throw new SlackApiException(new Response.Builder().build(), SLACK_EXCEPT_TEXT);
        }
        if (request.getChannel() != null && !VALID_CHANNEL_ID.equals(request.getChannel())) {
            lastFileResponse = new FilesUploadV2Response();
            lastFileResponse.setOk(false);
            lastFileResponse.setError(NOT_IN_CHANNEL_TEXT);
            return lastFileResponse;
        }
        // return an error response in some cases?
        File file = File.builder()
                .name(request.getFilename())
                .title(request.getTitle())
                .permalink(BASE_PERMA_LINK + request.getFilename())
                .urlPrivate(BASE_URL_PRIVATE + request.getFilename())
                .size(UPLOAD_FILE_SIZE)
                .build();
        lastFileResponse = new FilesUploadV2Response();
        lastFileResponse.setFile(file);
        lastFileResponse.setOk(true);
        return lastFileResponse;
    }

    @Override
    public ChatPostMessageResponse chatPostMessage(ChatPostMessageRequest request) throws IOException, SlackApiException {
        lastChatRequest = request;
        if (IO_EXCEPT_TEXT.equals(request.getText())) {
            throw new IOException(IO_EXCEPT_TEXT);
        }
        if (SLACK_EXCEPT_TEXT.equals(request.getText())) {
            throw new SlackApiException(new Response.Builder().build(), SLACK_EXCEPT_TEXT);
        }
        if (INVALID_TOKEN.equals(request.getToken())) {
            return lastChatResponse = CHAT_INVALID_TOKEN_RESPONSE;
        }
        if (INVALID_CHANNEL.equals(request.getChannel())) {
            return lastChatResponse = CHAT_INVALID_CHANNEL_RESPONSE;
        }
        if (ERROR_CHANNEL.equals(request.getChannel())) {
            lastChatResponse = new ChatPostMessageResponse();
            lastChatResponse.setOk(false);
            lastChatResponse.setError(request.getText());
            return lastChatResponse;
        }
        if (WARNING_CHANNEL.equals(request.getChannel())) {
            lastChatResponse = new ChatPostMessageResponse();
            lastChatResponse.setOk(true);
            lastChatResponse.setWarning(request.getText());
            return lastChatResponse;
        }
        lastChatResponse = new ChatPostMessageResponse();
        lastChatResponse.setOk(true);
        return lastChatResponse;
    }

    public ChatPostMessageRequest getLastChatPostMessageRequest() {
        return lastChatRequest;
    }

    public ChatPostMessageResponse getLastChatPostMessageResponse() {
        return lastChatResponse;
    }

    public ConversationsListRequest getLastConversationsListRequest() {
        return lastConvoRequest;
    }

    public ConversationsListResponse getLastConversationsListResponse() {
        return lastConvoResponse;
    }

    public FilesUploadV2Request getLastFilesUploadRequest() {
        return lastFileRequest;
    }

    public FilesUploadV2Response getLastFilesUploadResponse() {
        return lastFileResponse;
    }
}
