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

package org.apache.nifi.authorization.azure;

import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.graph.authentication.IAuthenticationProvider;
import com.microsoft.graph.http.IHttpRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientCrednetialAuthProvider implements IAuthenticationProvider {

    final private String authorityEndpoint; // { "Global", "https://login.microsoftonline.com/", "UsGovernment", "https://login.microsoftonline.us/" }
    final private String tenantId;
    final private String clientId;
    final private String clientSecret;
    private Date tokenExpiresOnDate;
    private String lastAcessToken;
    private final static String GRAPH_DEFAULT_SCOPE = "https://graph.microsoft.com/.default";
    private final static Logger logger = LoggerFactory.getLogger(ClientCrednetialAuthProvider.class);

    private ClientCrednetialAuthProvider(final Builder builder){
        this.authorityEndpoint = builder.getAuthorityEndpoint();
        this.tenantId = builder.getTenantId();
        this.clientId = builder.getClientId();
        this.clientSecret = builder.getClientSecret();
    }

    @Override
    public int hashCode() {
        return Objects.hash(authorityEndpoint, tenantId, clientId, clientSecret);
    }

    @Override
    public String toString() {
        return "{" +
            " authorityDNS='" + authorityEndpoint + "'" +
            ", tenantId='" + tenantId + "'" +
            ", clientId='" + clientId + "'" +
            ", clientSecret='" + clientSecret + "'" +
            "}";
    }

    public Date getLastAccessTokenExpirationDate(){
        return tokenExpiresOnDate;
    }

    private IAuthenticationResult getAccessTokenByClientCredentialGrant()
        throws MalformedURLException, ExecutionException, InterruptedException {

        ConfidentialClientApplication app = ConfidentialClientApplication.builder(
                this.clientId,
                ClientCredentialFactory.createFromSecret(this.clientSecret))
                .authority(String.format("%s/%s", authorityEndpoint, tenantId))
                .build();

        // With client credentials flows the scope is ALWAYS of the shape "resource/.default", as the
        // application permissions need to be set statically (in the portal), and then granted by a tenant administrator
        ClientCredentialParameters clientCredentialParam = ClientCredentialParameters.builder(
                Collections.singleton(GRAPH_DEFAULT_SCOPE))
                .build();

        CompletableFuture<IAuthenticationResult> future = app.acquireToken(clientCredentialParam);
        return future.get();
    }

    private String getAccessToken() {
        Date now = new Date();
        if ((lastAcessToken != null) && (tokenExpiresOnDate != null) && (tokenExpiresOnDate.getTime() -now.getTime() > 60000)) {
            return lastAcessToken;
        } else {
            try {
                IAuthenticationResult result = getAccessTokenByClientCredentialGrant();
                tokenExpiresOnDate = result.expiresOnDate(); // store this for token expiration checking
                lastAcessToken = result.accessToken();
            } catch(Exception ex) {
                logger.error("Failed to get access token", ex);
            }
            return lastAcessToken;
        }


    }

    @Override
    public void authenticateRequest(IHttpRequest request) {
        String accessToken = getAccessToken();
        if (accessToken != null) {
            request.addHeader("Authorization", "Bearer " + accessToken);
        }

    }

    public static class Builder {

        private String authorityEndpoint ="";
        private String tenantId = "";
        private String clientId = "";
        private String clientSecret = "";

        public Builder() {

        }

        public Builder authorityEndpoint(final String authorityEndpoint){
            this.authorityEndpoint = authorityEndpoint;
            return this;
        }

        public String getAuthorityEndpoint() {
            return this.authorityEndpoint;
        }

        public Builder tenantId(final String tenantId){
            this.tenantId = tenantId;
            return this;
        }

        public String getTenantId() {
            return this.tenantId;
        }

        public Builder clientId(final String clientId){
            this.clientId = clientId;
            return this;
        }

        public String getClientId() {
            return this.clientId;
        }

        public Builder clientSecret(final String clientSecret){
            this.clientSecret = clientSecret;
            return this;
        }

        public String getClientSecret() {
            return this.clientSecret;
        }

        @Override
        public int hashCode() {
            return Objects.hash(authorityEndpoint, tenantId, clientId, clientSecret);
        }

        @Override
        public String toString() {
            return "{" +
                " authorityDNS='" + getAuthorityEndpoint() + "'" +
                ", tenantId='" + getTenantId() + "'" +
                ", clientId='" + getClientId() + "'" +
                ", clientSecret='" + getClientSecret() + "'" +
                "}";
        }
        public ClientCrednetialAuthProvider build() {
            return new ClientCrednetialAuthProvider(this);
        }

    }
}