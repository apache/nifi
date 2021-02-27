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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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

public class ClientCredentialAuthProvider implements IAuthenticationProvider {

    private final String authorityEndpoint;
    private final String tenantId;
    private final String clientId;
    private final String clientSecret;
    private LocalDateTime tokenExpiresOnDate;
    private String lastAcessToken;
    private static final String GRAPH_DEFAULT_SCOPE = "https://graph.microsoft.com/.default";
    private static final Logger logger = LoggerFactory.getLogger(ClientCredentialAuthProvider.class);

    private ClientCredentialAuthProvider(final Builder builder){
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

    private IAuthenticationResult getAccessTokenByClientCredentialGrant()
        throws MalformedURLException, ExecutionException, InterruptedException {

        ConfidentialClientApplication app = ConfidentialClientApplication.builder(
                this.clientId,
                ClientCredentialFactory.createFromSecret(this.clientSecret))
                .authority(String.format("%s/%s", authorityEndpoint, tenantId))
                .build();
        ClientCredentialParameters clientCredentialParam = ClientCredentialParameters.builder(
                Collections.singleton(GRAPH_DEFAULT_SCOPE))
                .build();

        CompletableFuture<IAuthenticationResult> future = app.acquireToken(clientCredentialParam);
        return future.get();
    }

    private LocalDateTime convertToLocalDateTime(Date dateToConvert) {
        return Instant.ofEpochMilli(dateToConvert.getTime())
          .atZone(ZoneId.systemDefault())
          .toLocalDateTime();
    }

    private String getAccessToken() {
        if ((lastAcessToken != null) && (tokenExpiresOnDate != null) && (tokenExpiresOnDate.isAfter(LocalDateTime.now().plusMinutes(1)))) {
            return lastAcessToken;
        } else {
            try {
                IAuthenticationResult result = getAccessTokenByClientCredentialGrant();
                tokenExpiresOnDate = convertToLocalDateTime(result.expiresOnDate());
                lastAcessToken = result.accessToken();
            } catch(final Exception e) {
                logger.error("Failed to get access token due to {}", e.getMessage(), e);
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

        private String authorityEndpoint = "";
        private String tenantId = "";
        private String clientId = "";
        private String clientSecret = "";

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
        public ClientCredentialAuthProvider build() {
            return new ClientCredentialAuthProvider(this);
        }
    }
}
