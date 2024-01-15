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

package org.apache.nifi.processors.slack.util;

import com.slack.api.methods.MethodsClient;
import com.slack.api.methods.SlackApiException;
import com.slack.api.methods.request.conversations.ConversationsListRequest;
import com.slack.api.methods.response.conversations.ConversationsListResponse;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelMapper {

    private final Map<String, String> mappedChannels = new ConcurrentHashMap<>();
    private final MethodsClient client;

    public ChannelMapper(final MethodsClient client) {
        this.client = client;
    }

    public String lookupChannelId(String channelName) throws SlackApiException, IOException {
        if (channelName == null) {
            return null;
        }

        channelName = channelName.trim();
        if (channelName.startsWith("#")) {
            if (channelName.length() == 1) {
                return null;
            }

            channelName = channelName.substring(1);
        }

        final String channelId = mappedChannels.get(channelName);
        if (channelId == null) {
            lookupChannels(channelName);
        }

        return mappedChannels.get(channelName);
    }

    private void lookupChannels(final String desiredChannelName) throws SlackApiException, IOException {
        String cursor = null;
        while (true) {
            final ConversationsListRequest request = ConversationsListRequest.builder()
                .cursor(cursor)
                .limit(1000)
                .build();

            final ConversationsListResponse response = client.conversationsList(request);
            if (response.isOk()) {
                response.getChannels().forEach(channel -> mappedChannels.put(channel.getName(), channel.getId()));
                cursor = response.getResponseMetadata().getNextCursor();
                if (StringUtils.isEmpty(cursor)) {
                    return;
                }
                if (mappedChannels.containsKey(desiredChannelName)) {
                    return;
                }

                continue;
            }

            final String errorMessage = SlackResponseUtil.getErrorMessage(response.getError(), response.getNeeded(), response.getProvided(), response.getWarning());
            throw new RuntimeException("Failed to determine Channel IDs: " + errorMessage);
        }

    }
}
