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
import org.apache.nifi.web.security.token.LoginAuthenticationToken;

public interface OidcIdentityProvider {

    String OPEN_ID_CONNECT_SUPPORT_IS_NOT_CONFIGURED = "OpenId Connect support is not configured";

    /**
     * Initializes the provider.
     */
    void initializeProvider();

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
     * Returns the URI for the end session endpoint.
     *
     * @return uri for the end session endpoint
     */
    URI getEndSessionEndpoint();

    /**
     * Returns the URI for the revocation endpoint.
     *
     * @return uri for the revocation endpoint
     */
    URI getRevocationEndpoint();

    /**
     * Returns the scopes supported by the OIDC provider.
     *
     * @return support scopes
     */
    Scope getScope();

    /**
     * Exchanges the supplied authorization grant for a Login ID Token. Extracts the identity from the ID
     * token.
     *
     * @param authorizationGrant authorization grant for invoking the Token Endpoint
     * @return a Login Authentication Token
     * @throws IOException if there was an exceptional error while communicating with the OIDC provider
     */
    LoginAuthenticationToken exchangeAuthorizationCodeforLoginAuthenticationToken(AuthorizationGrant authorizationGrant) throws IOException;

    /**
     * Exchanges the supplied authorization grant for an Access Token.
     *
     * @param authorizationGrant authorization grant for invoking the Token Endpoint
     * @return an Access Token String
     * @throws Exception if there was an exceptional error while communicating with the OIDC provider
     */
    String exchangeAuthorizationCodeForAccessToken(AuthorizationGrant authorizationGrant) throws Exception;

    /**
     * Exchanges the supplied authorization grant for an ID Token.
     *
     * @param authorizationGrant authorization grant for invoking the Token Endpoint
     * @return an ID Token String
     * @throws IOException if there was an exceptional error while communicating with the OIDC provider
     */
    String exchangeAuthorizationCodeForIdToken(final AuthorizationGrant authorizationGrant) throws IOException;
}
