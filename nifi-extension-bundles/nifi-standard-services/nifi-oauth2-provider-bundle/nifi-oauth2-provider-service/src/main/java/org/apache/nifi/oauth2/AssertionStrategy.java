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
package org.apache.nifi.oauth2;

import org.apache.nifi.components.DescribedValue;

import java.util.Arrays;
import java.util.Optional;

/**
 * Supported strategies for producing the RFC 7523 JWT assertion presented to the token endpoint.
 */
public enum AssertionStrategy implements DescribedValue {
    SELF_SIGNED("Self-Signed", "Build and sign the JWT assertion locally using a Private Key Service."),
    EXTERNAL_PROVIDER("External Provider", "Use the token from an external OAuth2AccessTokenProvider directly as the JWT assertion.");

    private final String displayName;
    private final String description;

    AssertionStrategy(final String displayName, final String description) {
        this.displayName = displayName;
        this.description = description;
    }

    @Override
    public String getValue() {
        return displayName;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public static Optional<AssertionStrategy> fromValue(final String value) {
        if (value == null) {
            return Optional.empty();
        }

        return Arrays.stream(values())
                .filter(strategy -> strategy.getValue().equals(value))
                .findFirst();
    }
}
