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
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException;
import org.apache.nifi.properties.sensitive.SensitivePropertyProvider;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.vault.authentication.AppIdAuthentication;
import org.springframework.vault.authentication.AppIdAuthenticationOptions;
import org.springframework.vault.authentication.AppRoleAuthentication;
import org.springframework.vault.authentication.AppRoleAuthenticationOptions;
import org.springframework.vault.authentication.ClientAuthentication;
import org.springframework.vault.authentication.IpAddressUserId;
import org.springframework.vault.authentication.TokenAuthentication;
import org.springframework.vault.client.SimpleVaultEndpointProvider;
import org.springframework.vault.client.VaultClients;
import org.springframework.vault.client.VaultEndpoint;
import org.springframework.vault.client.VaultEndpointProvider;
import org.springframework.vault.config.ClientHttpRequestFactoryFactory;

import org.springframework.vault.core.VaultOperations;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.ClientOptions;
import org.springframework.vault.support.SslConfiguration;
import org.springframework.vault.support.VaultResponse;
import org.springframework.vault.support.VaultToken;
import org.springframework.vault.support.WrappedMetadata;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Sensitive properties using Vault Wrap and Unwrap operations.
 */
public class VaultSensitivePropertyProvider implements SensitivePropertyProvider {
    private static final Logger logger = LoggerFactory.getLogger(VaultSensitivePropertyProvider.class);

    static final String PROVIDER_NAME = "HashiCorp Vault Sensitive Property Provider";
    static final String VAULT_PARAM_DELIMITER = ",";
    static final String VAULT_KEY_DELIMITER = "/";
    static final String VAULT_PREFIX = "vault";
    static final String VAULT_AUTH_TOKEN = "token";
    static final String VAULT_AUTH_APP_ID = "appid";
    static final String VAULT_AUTH_APP_ROLE = "approle";
    static final Set<String> VAULT_AUTH_TYPES = new HashSet<>(Arrays.asList(VAULT_AUTH_TOKEN, VAULT_AUTH_APP_ID, VAULT_AUTH_APP_ROLE));

    private static final Duration DEFAULT_WRAP_DURATION = Duration.ofDays(365);
    private static String authType;
    private static VaultOperations vaultClient;
    private static String vaultParams;

    /**
     * Constructs a {@link SensitivePropertyProvider} that uses Vault wrap and unwrap.
     *
     * @param clientMaterial client authentication and configuration string
     */
    public VaultSensitivePropertyProvider(String clientMaterial) {
        authType = extractAuthType(clientMaterial);
        vaultParams = extractParams(clientMaterial);
        String vaultUri = extractUri(vaultParams);

        if (StringUtils.isBlank(authType) || StringUtils.isBlank(vaultParams) || StringUtils.isBlank(vaultUri)) {
            throw new SensitivePropertyProtectionException("Invalid Vault client material.");
        }

        RestTemplate restTemplate = getRestTemplate(vaultUri);
        ClientAuthentication vaultAuth;

        switch (authType) {
            case VAULT_AUTH_APP_ID:
                vaultAuth = getAppIdClient(restTemplate);
                break;
            case VAULT_AUTH_APP_ROLE:
                vaultAuth = getAppRoleClient(restTemplate);
                break;
            case VAULT_AUTH_TOKEN:
                vaultAuth = getTokenClient();
                break;
            default:
                throw new SensitivePropertyProtectionException("Unknown Vault authentication type.");
        }

        VaultEndpoint endpoint = VaultEndpoint.from(URI.create(vaultUri));
        vaultClient = new VaultTemplate(endpoint, vaultAuth);
    }

    private ClientAuthentication getTokenClient() {
        String token = extractToken(vaultParams);
        return new TokenAuthentication(token);
    }

    private ClientAuthentication getAppRoleClient(RestTemplate restTemplate) {
        String roleId = extractRoleId(vaultParams);
        String secretId = extractSecretId(vaultParams);

        if (StringUtils.isBlank(roleId) || StringUtils.isBlank(secretId)) {
            throw new SensitivePropertyProtectionException("Missing Vault app role value(s).");
        }
        AppRoleAuthenticationOptions appRoleOptions = AppRoleAuthenticationOptions.builder()
                .roleId(AppRoleAuthenticationOptions.RoleId.provided(roleId))
                .secretId(AppRoleAuthenticationOptions.SecretId.provided(secretId))
                .build();

        return new AppRoleAuthentication(appRoleOptions, restTemplate);
    }

    private ClientAuthentication getAppIdClient(RestTemplate restTemplate) {
        String appId = extractAppId(vaultParams);

        if (StringUtils.isBlank(appId)) {
            throw new SensitivePropertyProtectionException("Missing Vault app id value.");
        }
        AppIdAuthenticationOptions appIdOptions = AppIdAuthenticationOptions.builder()
                .appId(appId)
                .userIdMechanism(new IpAddressUserId()) // wha?
                .build();

        return new AppIdAuthentication(appIdOptions, restTemplate);
    }

    private RestTemplate getRestTemplate(String vaultUri) {
        RestOperations restOperations;
        ClientAuthentication vaultAuth;

        VaultEndpoint vaultEndpoint = null;
        try {
            vaultEndpoint = VaultEndpoint.from(new URI(vaultUri));
        } catch (URISyntaxException e) {
            throw new SensitivePropertyProtectionException(e);
        }

        VaultEndpointProvider vaultEndpointProvider = SimpleVaultEndpointProvider.of(vaultEndpoint);
        ClientOptions options = new ClientOptions();
        SslConfiguration sslConfig = SslConfiguration.unconfigured();
        ClientHttpRequestFactory clientHttpRequestFactory = ClientHttpRequestFactoryFactory.create(options, sslConfig);
        return VaultClients.createRestTemplate(vaultEndpointProvider, clientHttpRequestFactory);
    }

    /**
     * Extract the auth token from the decoded parameter string, the first parameter after the URI.
     *
     * Applies to token auth only.
     *
     * @param vaultParams decoded vault parameters string
     * @return auth token
     */
    private static String extractToken(String vaultParams) {
        String[] parts = vaultParams.split(VAULT_PARAM_DELIMITER, 2);
        if (parts.length >= 2) {
            return parts[1];
        }
        return null;
    }

    /**
     * Extract the app role id from the decoded parameter string, the first parameter after the URI.
     *
     * Applies to app role auth only.
     *
     * @param vaultParams decoded vault parameters string
     * @return app role id
     */
    private static String extractRoleId(String vaultParams) {
        String[] parts = vaultParams.split(VAULT_PARAM_DELIMITER, 3);
        if (parts.length >= 3) {
            return parts[1];
        }
        return null;
    }

    /**
     * Extract the app secret id from the decoded parameter string, the second parameter after the URI.
     *
     * Applies to app role auth only.
     *
     * @param vaultParams decoded vault parameters string
     * @return app secret id
     */
    private static String extractSecretId(String vaultParams) {
        String[] parts = vaultParams.split(VAULT_PARAM_DELIMITER, 3);
        if (parts.length >= 3) {
            return parts[2];
        }
        return null;
    }

    /**
     * Extract the app id from the decoded parameter string, the first parameter after the URI.
     *
     * Applies to app id auth only.
     *
     * @param vaultParams decoded vault parameters string
     * @return app id
     */
    private static String extractAppId(String vaultParams) {
        String[] parts = vaultParams.split(VAULT_PARAM_DELIMITER, 3);
        if (parts.length >= 2) {
            return parts[1];
        }
        return null;
    }

    /**
     * Extract the user id from the decoded parameter string, the second parameter after the URI.
     *
     * Applies to app id auth only.
     *
     * @param vaultParams decoded vault parameters string
     * @return user id
     */
    private static String extractUserId(String vaultParams) {
        String[] parts = vaultParams.split(VAULT_PARAM_DELIMITER, 3);
        if (parts.length >= 2) {
            return parts[2];
        }
        return null;
    }

    /**
     * Extract the Vault URI from the decoded parameter string, which is always the first value.
     *
     * Applies to all auth types.
     *
     * @param vaultParams decoded vault parameters string
     * @return Vault client URI
     */
    private static String extractUri(String vaultParams) {
        if (vaultParams == null) {
            return null;
        }
        String[] parts = vaultParams.split(VAULT_PARAM_DELIMITER, 2);
        if (parts.length >= 1) {
            return parts[0];
        }
        return null;
    }

    /**
     * Extract the Vault client parameters from the given key material.
     *
     * @param clientMaterial key in the form of "vault/auth-type/client-param,..."
     * @return decoded Vault client parameters
     */
    private static String extractParams(String clientMaterial) {
        String[] parts = clientMaterial.split(VAULT_KEY_DELIMITER, 3);
        if (parts.length >= 3) {
            return parts[2];
        }
        return null;
    }

    /**
     * Extract the Vault auth method from the given key material.
     *
     * @param clientMaterial key in the form of "vault/auth-type/client-param,..."
     * @return decoded Vault client auth method
     */
    private static String extractAuthType(String clientMaterial) {
        for (String authType : VAULT_AUTH_TYPES) {
            if (clientMaterial.startsWith(VAULT_PREFIX + VAULT_KEY_DELIMITER + authType + VAULT_KEY_DELIMITER)) {
                String[] parts = clientMaterial.split(VAULT_KEY_DELIMITER, 3);
                return parts[1];
            }
        }
        return null;
    }

    /**
     * Creates a Vault client material string for token authentication given a URI and token.
     */
    static String formatForToken(String uri, String token) {
        return VAULT_PREFIX + VAULT_KEY_DELIMITER + VAULT_AUTH_TOKEN + VAULT_KEY_DELIMITER + uri + VAULT_PARAM_DELIMITER + token;
    }

    /**
     * Creates a Vault client material string for app role authentication given a URI, role, and secret.
     */
    static String formatForAppRole(String uri, String roleId, String secretId) {
        String params = uri + VAULT_PARAM_DELIMITER + roleId + VAULT_PARAM_DELIMITER + secretId;
        return VAULT_PREFIX + VAULT_KEY_DELIMITER + VAULT_AUTH_APP_ROLE + VAULT_KEY_DELIMITER + params;
    }

    /**
     * Creates a Vault client material string for app id authentication given a URI, app id, and user id.
     */
    static String formatForAppId(String uri, String appId, String userId) {
        String params = uri + VAULT_PARAM_DELIMITER + appId + VAULT_PARAM_DELIMITER + userId;
        return VAULT_PREFIX + VAULT_KEY_DELIMITER + VAULT_AUTH_APP_ID + VAULT_KEY_DELIMITER + params;
    }

    /**
     * Returns the name of the underlying implementation.
     *
     * @return the name of this sensitive property provider
     */
    @Override
    public String getName() {
        return PROVIDER_NAME;

    }

    /**
     * Returns the key used to identify the provider implementation in {@code nifi.properties}.
     *
     * @return the key to persist in the sibling property
     */
    @Override
    public String getIdentifierKey() {
        return VAULT_PREFIX + VAULT_KEY_DELIMITER + authType + VAULT_KEY_DELIMITER + vaultParams;
    }

    /**
     * Returns the encrypted cipher text.
     *
     * @param unprotectedValue the sensitive value
     * @return the value to persist in the {@code nifi.properties} file
     * @throws SensitivePropertyProtectionException if there is an exception encrypting the value
     */
    @Override
    public String protect(String unprotectedValue) throws SensitivePropertyProtectionException {
        if (unprotectedValue == null || StringUtils.isBlank(unprotectedValue)) {
            throw new IllegalArgumentException("Cannot encrypt an empty value");
        }

        // Vault often throws an exception when wrapping a plain string payload, so we use a map instead:
        Map<String, String> plainMap = new HashMap<>();
        plainMap.put(VAULT_PREFIX, unprotectedValue);

        WrappedMetadata vaultResponse = vaultWrap(vaultClient, plainMap, DEFAULT_WRAP_DURATION);
        if (vaultResponse != null) {
            return vaultResponse.getToken().getToken();
        }

        throw new SensitivePropertyProtectionException("Empty response during wrap.");
    }

    /**
     * Returns the decrypted plaintext.
     *
     * @param protectedValue the cipher text read from the {@code nifi.properties} file
     * @return the raw value to be used by the application
     * @throws SensitivePropertyProtectionException if there is an error decrypting the cipher text
     */
    @Override
    public String unprotect(String protectedValue) throws SensitivePropertyProtectionException {
        VaultResponse vaultResponse = vaultUnwrap(vaultClient, protectedValue);
        if (vaultResponse == null) {
            throw new SensitivePropertyProtectionException("Empty response during unwrap.");
        }

        // The value was previously in a map, so here we pluck it out:
        Map<String, Object> responseData = vaultResponse.getData();
        if (responseData == null) {
            throw new SensitivePropertyProtectionException("Empty data in response.");
        }

        if (responseData.containsKey(VAULT_PREFIX)) {
            return responseData.get(VAULT_PREFIX).toString();
        }

        throw new SensitivePropertyProtectionException("Data key missing from response.");
    }

    /**
     * True when the client specifies a key like 'vault/token/...'.
     *
     * @param scheme name of encryption or protection scheme
     * @return true if this class can provide protected values
     */
    public static boolean isProviderFor(String scheme) {
        if (StringUtils.isBlank(scheme)) {
            return false;
        }

        String[] parts = scheme.split(VAULT_KEY_DELIMITER, 3);

        if (parts.length < 3) {
            return false;
        }

        if (!StringUtils.equals(parts[0], VAULT_PREFIX)) {
            return false;
        }

        return VAULT_AUTH_TYPES.contains(parts[1]);
    }

    /**
     * Returns a printable representation of this instance.
     *
     * @param clientMaterial Vault client material
     * @return printable string
     */
    public static String toPrintableString(String clientMaterial) {
        String authType = extractAuthType(clientMaterial);
        String hash = Hex.toHexString(clientMaterial.getBytes());
        String suffix = hash.substring(0, 8) + "..." + hash.substring(hash.length()-8, hash.length());

        return VAULT_PREFIX + VAULT_KEY_DELIMITER + authType + VAULT_KEY_DELIMITER + "hash:" + suffix;
    }

    /**
     * Unwraps the given vault (string) token.
     *
     * @param client Vault client
     * @param token previously wrapped token
     * @return response from Vault
     */
    static VaultResponse vaultUnwrap(VaultOperations client, String token) {
        return client.opsForWrapping().read(VaultToken.of(token));
    }

    /**
     * Wraps the given map.
     *
     * @param client Vault client
     * @param data arbitrary map of string to string
     * @param duration validity as Duration
     * @return wrapped data wrapped in more metadata
     */
    static WrappedMetadata vaultWrap(VaultOperations client, Map<String, String> data, Duration duration) {
        return client.opsForWrapping().wrap(data, duration);
    }

}
