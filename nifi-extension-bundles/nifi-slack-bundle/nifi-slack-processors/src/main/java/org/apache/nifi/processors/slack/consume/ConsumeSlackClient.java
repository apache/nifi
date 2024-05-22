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
package org.apache.nifi.processors.slack.consume;

import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.request.conversations.ConversationsHistoryRequest;
import com.slack.api.methods.request.conversations.ConversationsRepliesRequest;
import com.slack.api.methods.response.conversations.ConversationsHistoryResponse;
import com.slack.api.methods.response.conversations.ConversationsRepliesResponse;
import com.slack.api.methods.response.users.UsersInfoResponse;

import java.io.IOException;
import java.util.Map;


/**
 * A simple wrapper around the Slack Client with the methods that we use so that they can be easily mocked for testing
 */
public interface ConsumeSlackClient {
    ConversationsHistoryResponse fetchConversationsHistory(ConversationsHistoryRequest request) throws SlackApiException, IOException;

    ConversationsRepliesResponse fetchConversationsReplies(ConversationsRepliesRequest request) throws SlackApiException, IOException;

    UsersInfoResponse fetchUsername(String userId) throws SlackApiException, IOException;

    Map<String, String> fetchChannelIds() throws SlackApiException, IOException;

    String fetchChannelName(String channelId) throws SlackApiException, IOException;

}
