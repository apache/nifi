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
package org.apache.nifi.web.security.oidc;


import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.id.ClientID;

import java.io.IOException;
import java.net.URI;

public interface OidcIdentityProvider {

    String OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED = "OpenId Connect support is not configured";

    /**
     * Returns whether OIDC support is enabled.
     *
     * @return whether OIDC support is enabled
     */
    boolean isOidcEnabled();

    /**
     * Returns the configured client id.
     *
     * @return the client id
     */
    ClientID getClientId();

    /**
     * Returns the URI for the authorization endpoint.
     *
     * @return uri for the authorization endpoint
     */
    URI getAuthorizationEndpoint();

    /**
     * Returns the scopes supported by the OIDC provider.
     *
     * @return support scopes
     */
    Scope getScope();

    /**
     * Exchanges the supplied authorization grant for an ID token. Extracts the identity from the ID
     * token and converts it into NiFi JWT.
     *
     * @param authorizationGrant authorization grant for invoking the Token Endpoint
     * @return a NiFi JWT
     * @throws IOException if there was an exceptional error while communicating with the OIDC provider
     */
    String exchangeAuthorizationCode(AuthorizationGrant authorizationGrant) throws IOException;
}
