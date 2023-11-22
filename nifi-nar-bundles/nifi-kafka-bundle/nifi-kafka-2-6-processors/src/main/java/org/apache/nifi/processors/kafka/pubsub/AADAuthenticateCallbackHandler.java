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
package org.apache.nifi.processors.kafka.pubsub;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientCredential;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerToken;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerTokenCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.apache.nifi.kafka.shared.login.AADLoginConfigProvider.AZURE_TENANT_ID;
import static org.apache.nifi.kafka.shared.login.AADLoginConfigProvider.AZURE_APP_SECRET;
import static org.apache.nifi.kafka.shared.login.AADLoginConfigProvider.AZURE_APP_ID;
import static org.apache.nifi.kafka.shared.login.AADLoginConfigProvider.BOOTSTRAP_SERVER;

public class AADAuthenticateCallbackHandler implements AuthenticateCallbackHandler {

    private String authority;
    private String appId;
    private String appSecret;
    private String bootstrapServer;
    private ConfidentialClientApplication aadClient;
    private ClientCredentialParameters aadParameters;

    @Override
    public void configure(Map<String, ?> configs, String mechanism, List<AppConfigurationEntry> jaasConfigEntries) {
        final Optional<AppConfigurationEntry> configEntry = jaasConfigEntries.stream()
                .filter(j -> OAuthBearerLoginModule.class.getCanonicalName().equals(j.getLoginModuleName())).findFirst();
        Map<String, ?> options = configEntry.get().getOptions();
        this.authority = (String) options.get(AZURE_TENANT_ID);
        this.appId = (String) options.get(AZURE_APP_ID);
        this.appSecret = (String) options.get(AZURE_APP_SECRET);
        this.bootstrapServer = (String) options.get(BOOTSTRAP_SERVER);
        this.bootstrapServer = this.bootstrapServer.replaceAll("\\[|\\]", "");
        URI uri = URI.create("https://" + this.bootstrapServer);
        String sbUri = uri.getScheme() + "://" + uri.getHost();
        this.aadParameters =
                ClientCredentialParameters.builder(Collections.singleton(sbUri + "/.default"))
                        .build();

        this.authority = "https://login.microsoftonline.com/" + this.authority + "/";
    }

    @Override
    public void close() throws KafkaException {
        // NOOP
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback: callbacks) {
            if (callback instanceof OAuthBearerTokenCallback) {
                try {
                    OAuthBearerToken token = getOAuthBearerToken();
                    OAuthBearerTokenCallback oauthCallback = (OAuthBearerTokenCallback) callback;
                    oauthCallback.token(token);
                } catch (InterruptedException | ExecutionException | TimeoutException e) {
                    e.printStackTrace();
                }
            } else {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    OAuthBearerToken getOAuthBearerToken() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
        if (this.aadClient == null) {
            synchronized(this) {
                if (this.aadClient == null) {
                    IClientCredential credential = ClientCredentialFactory.createFromSecret(this.appSecret);
                    this.aadClient = ConfidentialClientApplication.builder(this.appId, credential)
                            .authority(this.authority)
                            .build();
                }
            }
        }

        IAuthenticationResult authResult = this.aadClient.acquireToken(this.aadParameters).get();

        return new OAuthBearerTokenImp(authResult.accessToken(), authResult.expiresOnDate());
    }

}

