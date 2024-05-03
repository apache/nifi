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

import com.slack.api.methods.response.users.UsersInfoResponse;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.slack.util.SlackResponseUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class UsernameLookup {
    private final ConcurrentMap<String, String> userIdToNameMapping = new ConcurrentHashMap<>();
    private final ConsumeSlackClient client;
    private final ComponentLog logger;

    public UsernameLookup(final ConsumeSlackClient client, final ComponentLog logger) {
        this.client = client;
        this.logger = logger;
    }

    public String getUsername(final String userId) {
        final String cachedUsername = userIdToNameMapping.get(userId);
        if (cachedUsername != null) {
            return cachedUsername;
        }

        try {
            final UsersInfoResponse response = client.fetchUsername(userId);
            if (response.isOk()) {
                final String username = response.getUser().getName();
                userIdToNameMapping.put(userId, username);
                return username;
            }

            final String errorMessage = SlackResponseUtil.getErrorMessage(response.getError(), response.getNeeded(), response.getProvided(), response.getWarning());
            logger.warn("Failed to retrieve Username for User ID {}: {}", userId, errorMessage);
            return null;
        } catch (final Exception e) {
            if (SlackResponseUtil.isRateLimited(e)) {
                logger.warn("Failed to retrieve Username for User ID {} because the Rate Limit has been exceeded", userId);
            } else {
                logger.warn("Failed to retrieve Username for User ID {}: {}", userId, e.getMessage(), e);
            }

            return null;
        }
    }
}
