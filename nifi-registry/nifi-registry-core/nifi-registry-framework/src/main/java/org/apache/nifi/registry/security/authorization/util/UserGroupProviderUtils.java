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
package org.apache.nifi.registry.security.authorization.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.security.authorization.AuthorizerConfigurationContext;
import org.apache.nifi.registry.security.identity.IdentityMapper;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility methods related to access policies for use by various {@link org.apache.nifi.registry.security.authorization.UserGroupProvider} implementations.
 */
public final class UserGroupProviderUtils {

    public static final String PROP_INITIAL_USER_IDENTITY_PREFIX = "Initial User Identity ";
    public static final Pattern INITIAL_USER_IDENTITY_PATTERN = Pattern.compile(PROP_INITIAL_USER_IDENTITY_PREFIX + "\\S+");

    public static Set<String> getInitialUserIdentities(final AuthorizerConfigurationContext configurationContext, final IdentityMapper identityMapper) {
        final Set<String> initialUserIdentities = new HashSet<>();
        for (Map.Entry<String,String> entry : configurationContext.getProperties().entrySet()) {
            Matcher matcher = UserGroupProviderUtils.INITIAL_USER_IDENTITY_PATTERN.matcher(entry.getKey());
            if (matcher.matches() && !StringUtils.isBlank(entry.getValue())) {
                initialUserIdentities.add(identityMapper.mapUser(entry.getValue()));
            }
        }
        return initialUserIdentities;
    }

    private UserGroupProviderUtils() {

    }
}
