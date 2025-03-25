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

public enum TokenRefreshStrategy implements DescribedValue {

    ON_TOKEN_EXPIRATION("The token will be refreshed based on its expiration time and the configured refresh window"),

    ON_UNAUTHORIZED_RESPONSE("A new token will be requested in case of a non-authorized request (HTTP 401) even if the current token has not expired");

    private final String description;

    TokenRefreshStrategy(final String description) {
        this.description = description;
    }

    @Override
    public String getValue() {
        return name();
    }

    @Override
    public String getDisplayName() {
        return name();
    }

    @Override
    public String getDescription() {
        return description;
    }

}
