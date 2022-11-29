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
package org.apache.nifi.web.security.cookie;

import org.apache.nifi.web.security.http.SecurityCookieName;

/**
 * Application Cookie Names
 */
public enum ApplicationCookieName {
    /** Authorization Bearer contains signed JSON Web Token and requires Strict Same Site handling */
    AUTHORIZATION_BEARER(SecurityCookieName.AUTHORIZATION_BEARER.getName(), SameSitePolicy.STRICT),

    /** Cross-Site Request Forgery mitigation token requires Strict Same Site handling */
    REQUEST_TOKEN(SecurityCookieName.REQUEST_TOKEN.getName(), SameSitePolicy.STRICT),

    /** Logout Requests can interact with external identity providers requiring no Same Site restrictions */
    LOGOUT_REQUEST_IDENTIFIER("__Secure-Logout-Request-Identifier", SameSitePolicy.NONE),

    /** OpenID Connect Requests use external identity providers requiring no Same Site restrictions */
    OIDC_REQUEST_IDENTIFIER("__Secure-OIDC-Request-Identifier", SameSitePolicy.NONE),

    /** SAML Requests use external identity providers requiring no Same Site restrictions */
    SAML_REQUEST_IDENTIFIER("__Secure-SAML-Request-Identifier", SameSitePolicy.NONE);

    private final String cookieName;

    private final SameSitePolicy sameSitePolicy;

    ApplicationCookieName(final String cookieName, final SameSitePolicy sameSitePolicy) {
        this.cookieName = cookieName;
        this.sameSitePolicy = sameSitePolicy;
    }

    public String getCookieName() {
        return cookieName;
    }

    public SameSitePolicy getSameSitePolicy() {
        return sameSitePolicy;
    }
}
