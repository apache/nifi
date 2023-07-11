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
package org.apache.nifi.web.security.oidc.userinfo;

import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.core.oidc.OidcUserInfo;
import org.springframework.security.oauth2.core.oidc.user.DefaultOidcUser;

import java.util.Collection;
import java.util.Objects;

/**
 * Standard extension of Spring Security OIDC User supporting customized name configuration
 */
class StandardOidcUser extends DefaultOidcUser {
    private final String name;

    /**
     * Standard OIDC User constructor with required parameters and customized name value for identification
     *
     * @param authorities Granted Authorities
     * @param idToken OIDC ID Token
     * @param userInfo OIDC User Information
     * @param nameAttributeKey Claim name that parent class uses to determine username for identification
     * @param name Customized name identifying the user
     */
    public StandardOidcUser(
            final Collection<? extends GrantedAuthority> authorities,
            final OidcIdToken idToken,
            final OidcUserInfo userInfo,
            final String nameAttributeKey,
            final String name
    ) {
        super(authorities, idToken, userInfo, nameAttributeKey);
        this.name = Objects.requireNonNull(name, "Name required");
    }

    @Override
    public String getName() {
        return name;
    }
}
