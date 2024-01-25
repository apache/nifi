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
import com.slack.api.model.User;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.slack.util.SlackResponseUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class UserDetailsLookup {
    private final ConcurrentMap<String, User> userIdToInfoMapping = new ConcurrentHashMap<>();
    private final UserInfoClient client;
    private final ComponentLog logger;

    public UserDetailsLookup(final UserInfoClient client, final ComponentLog logger) {
        this.client = client;
        this.logger = logger;
    }

    public User getUserDetails(final String userId) {
        final User cachedUserInfo = userIdToInfoMapping.get(userId);
        if (cachedUserInfo != null) {
            return cachedUserInfo;
        }

        try {
            final UsersInfoResponse response = client.fetchUserInfo(userId);
            if (response.isOk()) {
                final User user = response.getUser();
                userIdToInfoMapping.put(userId, user);
                return user;
            }

            final String errorMessage = SlackResponseUtil.getErrorMessage(response.getError(), response.getNeeded(), response.getProvided(), response.getWarning());
            logger.warn("Failed to retrieve user details for User ID {}: {}", userId, errorMessage);
            return null;
        } catch (final Exception e) {
            if (SlackResponseUtil.isRateLimited(e)) {
                logger.warn("Failed to retrieve user details for User ID {} because the Rate Limit has been exceeded", userId);
            } else {
                logger.warn("Failed to retrieve user details for User ID {}: {}", userId, e.getMessage(), e);
            }

            return null;
        }
    }

}
