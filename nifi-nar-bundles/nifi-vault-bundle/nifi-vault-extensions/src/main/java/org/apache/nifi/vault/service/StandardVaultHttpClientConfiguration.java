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
package org.apache.nifi.vault.service;

import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.HttpClients;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.ssl.SSLContextService;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.vault.authentication.AppIdAuthentication;
import org.springframework.vault.authentication.AppIdAuthenticationOptions;
import org.springframework.vault.authentication.AppRoleAuthentication;
import org.springframework.vault.authentication.AppRoleAuthenticationOptions;
import org.springframework.vault.authentication.ClientAuthentication;
import org.springframework.vault.authentication.IpAddressUserId;
import org.springframework.vault.authentication.StaticUserId;
import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.VaultClients;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.config.AbstractVaultConfiguration;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.net.URI;

/**
 * Concrete Vault HTTP Client Configuration.  This adapts a controller service to the functionality expected by the
 * Spring Vault package.
 *
 */
public class StandardVaultHttpClientConfiguration extends AbstractVaultConfiguration {
    public static final String AUTH_TYPE_APP_ID = "app-id";
    public static final String AUTH_TYPE_TOKEN = "token";
    public static final String AUTH_TYPE_APP_ROLE = "app-role";
    public static final String APP_ID_STATIC_USER_ID = "static-user-id";
    public static final String APP_ID_IP_ADDRESS = "ip-address";

    /**
     * Supported auth types.
     *
     * @return array of allowable auth types.
     */
    static AllowableValue[] getAvailableAuthTypes() {
        return new AllowableValue[]{
                new AllowableValue(AUTH_TYPE_TOKEN, "Token Authentication", "Token authentication type"),
                new AllowableValue(AUTH_TYPE_APP_ID, "App Id Authentication", "App Id authentication type"),
                new AllowableValue(AUTH_TYPE_APP_ROLE, "App Role Authentication", "App role authentication type"),
        };
    }

    /**
     * Supported app id "user id mechanisms".
     *
     * @return array of allowable user id mechanisms.
     */
    static AllowableValue[] getAppIdMechanisms() {
        return new AllowableValue[]{
                new AllowableValue(APP_ID_STATIC_USER_ID, "User Id", "User Id"),
                new AllowableValue(APP_ID_IP_ADDRESS,  "Host IP", "Host IP Address"),
        };
    }

    private final ConfigurationContext configContext;
    private SSLContext sslContext;

    /**
     * Constructor.  Only the controller's configuration context is needed to operate, not the controller itself.
     *
     * @param configContext Controller Service {@link ConfigurationContext}.
     */
    StandardVaultHttpClientConfiguration(ConfigurationContext configContext) {
        this.configContext = configContext;
    }

    /**
     * Builds a Vault endpoint for the URI from the current config context.
     *
     * @return {@link VaultEndpoint}
     */
    public VaultEndpoint vaultEndpoint() {
        String serverUrl = configContext.getProperty(VaultHttpClientControllerService.URL).getValue();
        return VaultEndpoint.from(URI.create(serverUrl));
    }

    /**
     * Builds a Vault {@link ClientAuthentication} from the current config context.
     *
     * @return {@link ClientAuthentication}
     */
    public ClientAuthentication clientAuthentication() {
        String authType = configContext.getProperty(VaultHttpClientControllerService.AUTH_TYPE).getValue();
        RestTemplate template = VaultClients.createRestTemplate(vaultEndpoint(), createRequestFactory());

        switch (authType) {
            case AUTH_TYPE_TOKEN:
                return new TokenAuthentication(configContext.getProperty(VaultHttpClientControllerService.TOKEN).getValue());

            case AUTH_TYPE_APP_ID: {
                String appId = configContext.getProperty(VaultHttpClientControllerService.APP_ID).getValue();
                AppIdAuthenticationOptions options;

                switch (configContext.getProperty(VaultHttpClientControllerService.USER_ID_MECHANISM).getValue()) {
                    case APP_ID_STATIC_USER_ID: {
                        String userId = configContext.getProperty(VaultHttpClientControllerService.USER_ID).getValue();
                        options = AppIdAuthenticationOptions.builder().userIdMechanism(new StaticUserId(userId)).appId(appId).build();
                        return new AppIdAuthentication(options, template);
                    }
                    case APP_ID_IP_ADDRESS: {
                        options = AppIdAuthenticationOptions.builder().userIdMechanism(new IpAddressUserId()).appId(appId).build();
                        return new AppIdAuthentication(options, template);
                    }
                    default:
                        throw new RuntimeException("Unsupported App Id user id mechanism.");
                }
            }

            case AUTH_TYPE_APP_ROLE: {
                String roleId = configContext.getProperty(VaultHttpClientControllerService.ROLE_ID).getValue();
                String secretId = configContext.getProperty(VaultHttpClientControllerService.SECRET_ID).getValue();
                AppRoleAuthenticationOptions options = AppRoleAuthenticationOptions.builder()
                        .roleId(AppRoleAuthenticationOptions.RoleId.provided(roleId))
                        .secretId(AppRoleAuthenticationOptions.SecretId.provided(secretId))
                        .build();
                return new AppRoleAuthentication(options, template);
            }

            default:
                throw new RuntimeException("Client auth type not supported");
        }
    }

    /**
     * Builds a {@link HttpComponentsClientHttpRequestFactory} from the current config context.
     *
     * @return {@link HttpComponentsClientHttpRequestFactory}, optionally with an SSL context built from the config context.
     */
    HttpComponentsClientHttpRequestFactory createRequestFactory() {
        String scheme = URI.create(configContext.getProperty(VaultHttpClientControllerService.URL).getValue()).getScheme();
        HttpComponentsClientHttpRequestFactory requestFactory = new HttpComponentsClientHttpRequestFactory();

        if (scheme.equalsIgnoreCase("https")) {
            requestFactory.setHttpClient(HttpClients.custom().setSSLSocketFactory(new SSLConnectionSocketFactory(getSSLContext())).build());
        } else if (!scheme.equalsIgnoreCase("http")) {
            throw new RuntimeException("HTTP or HTTPS URL scheme required.");
        }
        return requestFactory;
    }

    /**
     * Builds an {@link SSLContext} from the current config context.
     *
     * @return {@link SSLContext}, possibly cached
     */
    private SSLContext getSSLContext() {
        if (sslContext == null) {
            if (configContext == null || !configContext.getProperty(VaultHttpClientControllerService.SSL_CONTEXT_SERVICE).isSet())
                throw new RuntimeException("SSL Context Service required for HTTPS connections");

            sslContext = configContext.getProperty(VaultHttpClientControllerService.SSL_CONTEXT_SERVICE)
                    .asControllerService(SSLContextService.class)
                    .createSSLContext(SSLContextService.ClientAuth.REQUIRED);
        }
        return sslContext;
    }
}
