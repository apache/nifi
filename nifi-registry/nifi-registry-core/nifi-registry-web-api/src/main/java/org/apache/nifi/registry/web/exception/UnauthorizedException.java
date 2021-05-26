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
package org.apache.nifi.registry.web.exception;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.registry.security.authentication.IdentityProviderUsage;

import java.util.List;

/**
 * An exception for a convenient way to create a 401 Unauthorized response
 * using an exception mapper
 */
public class UnauthorizedException extends RuntimeException {

    private String[] wwwAuthenticateChallenge;

    public UnauthorizedException() {
    }

    public UnauthorizedException(String message) {
        super(message);
    }

    public UnauthorizedException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnauthorizedException(Throwable cause) {
        super(cause);
    }

    public UnauthorizedException withAuthenticateChallenge(IdentityProviderUsage.AuthType authType) {
        wwwAuthenticateChallenge = new String[] { authType.getHttpAuthScheme() };
        return this;
    }

    public UnauthorizedException withAuthenticateChallenge(List<IdentityProviderUsage.AuthType> authTypes) {
        wwwAuthenticateChallenge = new String[authTypes.size()];
        for (int i = 0; i < authTypes.size(); i++) {
            wwwAuthenticateChallenge[i] = authTypes.get(i).getHttpAuthScheme();
        }
        return this;
    }

    public UnauthorizedException withAuthenticateChallenge(String authType) {
        wwwAuthenticateChallenge = new String[] { authType };
        return this;
    }

    public UnauthorizedException withAuthenticateChallenge(String[] authTypes) {
        wwwAuthenticateChallenge = authTypes;
        return this;
    }

    public String getWwwAuthenticateChallenge() {
        return StringUtils.join(wwwAuthenticateChallenge, ",");
    }

}
