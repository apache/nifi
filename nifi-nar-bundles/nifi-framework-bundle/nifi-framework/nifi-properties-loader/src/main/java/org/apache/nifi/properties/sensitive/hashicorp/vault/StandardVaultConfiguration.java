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
package org.apache.nifi.properties.sensitive.hashicorp.vault;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.sensitive.ExternalProperties;
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException;

import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import org.springframework.vault.authentication.AppIdAuthentication;
import org.springframework.vault.authentication.AppIdAuthenticationOptions;
import org.springframework.vault.authentication.AppRoleAuthentication;
import org.springframework.vault.authentication.AppRoleAuthenticationOptions;
import org.springframework.vault.authentication.ClientAuthentication;
import org.springframework.vault.authentication.CubbyholeAuthentication;
import org.springframework.vault.authentication.CubbyholeAuthenticationOptions;
import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.VaultClients;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.config.AbstractVaultConfiguration;
import org.springframework.vault.config.ClientHttpRequestFactoryFactory;
import org.springframework.vault.config.EnvironmentVaultConfiguration;
import org.springframework.vault.support.SslConfiguration;
import org.springframework.vault.support.VaultToken;

import java.io.File;
import java.security.KeyStore;


// This class provides our configuration to the Vault client library.  Some of the implementation is inspired by / cribbed from the
// base class.
class StandardVaultConfiguration extends EnvironmentVaultConfiguration {
    private static final String VAULT_AUTH_TOKEN = "token";
    private static final String VAULT_AUTH_APP_ID = "appid";
    private static final String VAULT_AUTH_APP_ROLE = "approle";
    private static final String VAULT_AUTH_CUBBYHOLE = "cubbyhole";

    private final VaultEndpoint endpoint;
    private final ExternalProperties propertyProvider;

    StandardVaultConfiguration(VaultEndpoint vaultEndpoint, ExternalProperties externalProperties) {
        endpoint = vaultEndpoint;
        propertyProvider = externalProperties;
    }

    // Generates the client auth for token auth
    @Override
    protected ClientAuthentication tokenAuthentication() {
        return new TokenAuthentication(getVaultToken());
    }

    // Generates the client auth for app id auth
    @Override
    protected ClientAuthentication appIdAuthentication() {
        String appId = getVaultAppId();
        String userId = getVaultUserId();

        if (StringUtils.isBlank(appId) || StringUtils.isBlank(userId)) {
            throw new SensitivePropertyProtectionException("Missing Vault App ID authentication values.  Must supply app id and user id.");
        }

        AppIdAuthenticationOptions appIdOptions = AppIdAuthenticationOptions.builder()
                .appId(appId)
                .userIdMechanism(getAppIdUserIdMechanism(userId))
                .build();

        return new AppIdAuthentication(appIdOptions, VaultClients.createRestTemplate(endpoint, clientHttpRequestFactoryWrapper().getClientHttpRequestFactory()));
    }

    // Returns the client http request factory wrapper with our options and SSL config.
    @Override
    public AbstractVaultConfiguration.ClientFactoryWrapper clientHttpRequestFactoryWrapper() {
        return new AbstractVaultConfiguration.ClientFactoryWrapper(ClientHttpRequestFactoryFactory.create(this.clientOptions(), this.sslConfiguration()));
    }

    // Generates the client auth for app role auth.
    @Override
    protected ClientAuthentication appRoleAuthentication() {
        final String roleId = getVaultRoleId();
        final String secretId = getVaultSecretId();

        if (StringUtils.isBlank(roleId) || StringUtils.isBlank(secretId)) {
            throw new SensitivePropertyProtectionException("Missing Vault App Role authentication values.  Must supply role id and secret id.");
        }

        final AppRoleAuthenticationOptions appRoleOptions = AppRoleAuthenticationOptions.builder()
                .roleId(roleId)
                .secretId(secretId)
                .build();

        return new AppRoleAuthentication(appRoleOptions, VaultClients.createRestTemplate(endpoint, clientHttpRequestFactoryWrapper().getClientHttpRequestFactory()));
    }

    // Generates the client auth for cubbyhole auth.
    @Override
    protected ClientAuthentication cubbyholeAuthentication() {
        String token = getVaultToken();
        CubbyholeAuthenticationOptions options = CubbyholeAuthenticationOptions
                .builder()
                .initialToken(VaultToken.of(token))
                .path("cubbyhole/token")
                .build();

        return new CubbyholeAuthentication(options, VaultClients.createRestTemplate(endpoint, clientHttpRequestFactoryWrapper().getClientHttpRequestFactory()));
    }

    // Generates a Vault client auth object based on the "vault.authentication" property
    @Override
    public ClientAuthentication clientAuthentication() {
        switch (getVaultAuthentication()) {
            case VAULT_AUTH_APP_ID:
                return appIdAuthentication();
            case VAULT_AUTH_APP_ROLE:
                return appRoleAuthentication();
            case VAULT_AUTH_TOKEN:
                return tokenAuthentication();
            case VAULT_AUTH_CUBBYHOLE:
                return cubbyholeAuthentication();
            default:
                throw new SensitivePropertyProtectionException("Unknown Vault authentication type.");
        }
        // Not implemented:
        // AwsEc2Authentication
        // AwsIamAuthentication
        // ClientCertificateAuthentication
        // LoginTokenAdapter
    }

    // Generates an SSL config object based on the various "vault.ssl.*" properties
    @Override
    public SslConfiguration sslConfiguration() {
        Resource keyStore = getVaultSslKeyStore();
        char[] keyStorePassword = getVaultSslKeyStorePassword();
        Resource trustStore = getVaultSslTrustStore();
        char[] trustStorePassword = getVaultSslTrustStorePassword();

        return new SslConfiguration(
                new SslConfiguration.KeyStoreConfiguration(keyStore, keyStorePassword, KeyStore.getDefaultType()),
                new SslConfiguration.KeyStoreConfiguration(trustStore, trustStorePassword, KeyStore.getDefaultType()));
    }

    /**
     * Extract the auth token from the external properties.
     *
     * @return vault token
     */
    private String getVaultToken() {
        return this.propertyProvider.get("vault.token");
    }

    /**
     * Extract the app role id from the external properties.
     *
     * @return app role id
     */
    private String getVaultRoleId() {
        return this.propertyProvider.get("vault.app-role.role-id");
    }

    /**
     * Extract the app secret id from the external properties.
     *
     * @return app secret id
     */
    private String getVaultSecretId() {
        return this.propertyProvider.get("vault.app-role.secret-id");
    }

    /**
     * Extract the app id from the external properties.
     *
     * @return app id
     */
    private String getVaultAppId() {
        return this.propertyProvider.get("vault.app-id.app-id");
    }

    /**
     * Extract the user id from the external properties.
     *
     * @return user id
     */
    private String getVaultUserId() {
        return this.propertyProvider.get("vault.app-id.user-id");
    }

    /**
     * Extract the Vault auth method from the external properties.
     *
     * @return auth method
     */
    private String getVaultAuthentication() {
        return this.propertyProvider.get("vault.authentication");
    }

    Resource getVaultSslTrustStore() {
        String filename = this.propertyProvider.get("vault.ssl.trust-store");
        return StringUtils.isBlank(filename) ? null : new FileSystemResource(new File(filename));
    }

    char[] getVaultSslTrustStorePassword() {
        String password = this.propertyProvider.get("vault.ssl.trust-store-password");
        return StringUtils.isBlank(password) ? null : password.toCharArray();
    }

    Resource getVaultSslKeyStore() {
        String filename = this.propertyProvider.get("vault.ssl.key-store");
        return StringUtils.isBlank(filename) ? null : new FileSystemResource(new File(filename));
    }

    char[] getVaultSslKeyStorePassword() {
        String password = this.propertyProvider.get("vault.ssl.key-store-password");
        return StringUtils.isBlank(password) ? null : password.toCharArray();
    }

    // This method creates an empty spring environment.
    @Override
    protected Environment getEnvironment() {
        return new Environment() {
            @Override
            public String[] getActiveProfiles() {
                return new String[0];
            }

            @Override
            public String[] getDefaultProfiles() {
                return new String[0];
            }

            @Override
            public boolean acceptsProfiles(String... strings) {
                return false;
            }

            @Override
            public boolean containsProperty(String s) {
                return false;
            }

            @Override
            public String getProperty(String s) {
                return null;
            }

            @Override
            public String getProperty(String s, String s1) {
                return null;
            }

            @Override
            public <T> T getProperty(String s, Class<T> aClass) {
                return null;
            }

            @Override
            public <T> T getProperty(String s, Class<T> aClass, T t) {
                return null;
            }

            @Override
            public <T> Class<T> getPropertyAsClass(String s, Class<T> aClass) {
                return null;
            }

            @Override
            public String getRequiredProperty(String s) throws IllegalStateException {
                return null;
            }

            @Override
            public <T> T getRequiredProperty(String s, Class<T> aClass) throws IllegalStateException {
                return null;
            }

            @Override
            public String resolvePlaceholders(String s) {
                return null;
            }

            @Override
            public String resolveRequiredPlaceholders(String s) throws IllegalArgumentException {
                return null;
            }
        };
    }
}
