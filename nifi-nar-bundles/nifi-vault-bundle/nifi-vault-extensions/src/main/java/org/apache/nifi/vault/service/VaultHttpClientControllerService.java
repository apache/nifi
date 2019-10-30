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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.vault.VaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.vault.authentication.SimpleSessionManager;
import org.springframework.vault.core.VaultKeyValueOperations;
import org.springframework.vault.core.VaultKeyValueOperationsSupport;
import org.springframework.vault.core.VaultTemplate;
import org.springframework.vault.support.VaultMount;
import org.springframework.vault.support.VaultResponse;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;


/**
 * Simple controller service for interacting with a Hashicorp Vault server.
 *
 * */
@Tags({"vault", "secret", "secure"})
@CapabilityDescription("A service that provides operations to interact with a Hashicorp Vault instance.")
public class VaultHttpClientControllerService extends AbstractControllerService implements VaultHttpClient {
    private static final Logger logger = LoggerFactory.getLogger(VaultHttpClientControllerService.class);

    // Vault server URL is always required:
    public static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("url")
            .displayName("Vault URL")
            .description("Vault server URL")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .sensitive(false)
            .build();

    // We always require the path to the "secrets engine":
    public static final PropertyDescriptor SECRETS_PATH = new PropertyDescriptor.Builder()
            .name("secrets-path")
            .displayName("Secrets Path")
            .description("Secrets engine path")
            .defaultValue("kv")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .sensitive(false)
            .build();

    // We always require a prefix to use with KV operations:
    public static final PropertyDescriptor KV_PREFIX = new PropertyDescriptor.Builder()
            .name("kv-prefix")
            .displayName("Key Value Store Prefix")
            .description("Key value store path prefix")
            .defaultValue("/")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .sensitive(false)
            .build();

    // Auth type is always required:
    public static final PropertyDescriptor AUTH_TYPE = new PropertyDescriptor.Builder()
            .name("auth-type")
            .displayName("Auth Type")
            .description("Authentication type")
            .allowableValues(StandardVaultHttpClientConfiguration.getAvailableAuthTypes())
            .defaultValue(StandardVaultHttpClientConfiguration.getAvailableAuthTypes()[0].getValue())
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .sensitive(false)
            .build();

    // The remaining properties are optional, or required by an option from a property above.

    // We need one property for token auth:
    public static final PropertyDescriptor TOKEN = new PropertyDescriptor.Builder()
            .name("token")
            .displayName("Auth Token")
            .description("Auth Token material used during 'Token Authentication'")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(true)
            .build();

    // For app id auth, we need three props: one for the app id, one for the user id mechanism, one for the static user id (if any)
    public static final PropertyDescriptor APP_ID = new PropertyDescriptor.Builder()
            .name("app-id")
            .displayName("App Id")
            .description("App id used during 'App Id Authentication'")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor USER_ID_MECHANISM = new PropertyDescriptor.Builder()
            .name("user-id-mech")
            .displayName("User Id Mechanism")
            .description("User id mechanism used during 'App Id Authentication'")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .allowableValues(StandardVaultHttpClientConfiguration.getAppIdMechanisms())
            .defaultValue(StandardVaultHttpClientConfiguration.getAppIdMechanisms()[0].getValue())
            .required(false)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor USER_ID = new PropertyDescriptor.Builder()
            .name("user-id")
            .displayName("User Id")
            .description("User id used during 'App Id Authentication'")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(true)
            .build();

    // For App Role auth, we need role id and secret id:
    public static final PropertyDescriptor ROLE_ID = new PropertyDescriptor.Builder()
            .name("role-id")
            .displayName("Role Id")
            .description("The app role id to use during 'App Role Authentication'")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor SECRET_ID = new PropertyDescriptor.Builder()
            .name("secret-id")
            .displayName("Secret Id")
            .description("The secret id to use during 'App Role Authentication'")
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .sensitive(false)
            .build();

    // We defer SSL contexts to a service:
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("ssl-context-service")
            .displayName("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL connections")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    private VaultTemplate cachedServerOperations;
    private VaultKeyValueOperationsSupport.KeyValueBackend cachedKvVersion;
    private VaultKeyValueOperations cachedKvOperations;

    /**
     * Supported property descriptors.
     *
     * @return {@link List} of {@link PropertyDescriptor}.
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(
                // required
                URL, SECRETS_PATH, KV_PREFIX, AUTH_TYPE,
                // token auth
                TOKEN,
                // app id auth
                APP_ID, USER_ID_MECHANISM, USER_ID,
                // app role auth
                ROLE_ID, SECRET_ID,
                // remaining
                SSL_CONTEXT_SERVICE);
    }

    /**
     * Resets this service from a context.
     *
     * @param configurationContext {@link ConfigurationContext}
     */
    @OnEnabled
    public void resetFromContext(ConfigurationContext configurationContext) throws InitializationException {
        this.abstractStoreConfigContext(configurationContext);
        try {
            this.getKvOperations();
        } catch (final Throwable t) {
            throw new InitializationException(t);
        }
    }

    /**
     * Disables this service.
     */
    @OnDisabled
    public void onDisabled() {
        this.cachedKvVersion = null;
        this.cachedKvOperations = null;
        this.cachedServerOperations = null;
    }

    /**
     * This implementation writes dynamic properties to the KV store.
     *
     * @param descriptor of the modified property
     * @param oldValue non-null property value (previous)
     * @param newValue the new property value or if null indicates the property
     */
    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (descriptor.isDynamic()) {
            String key = descriptor.getName();
            String prefix = getProperty(KV_PREFIX).getValue();
            Map<String, Object> output = null;
            try {
                output = read(prefix);
            } catch (final IOException e) {
                logger.error("Failed to read dynamic property set for {}", key);
                return;
            }

            if (output != null) {
                output.put(key, newValue);
                    try {
                    write(prefix, output);
                } catch (final IOException e) {
                    logger.error("Failed to write dynamic property {}", key);
                }
            } else {
                logger.warn("Failed to update dynamic property key {}", key);
            }
        }
    }

    /**
     * Gets a dynamic property descriptor by name.
     *
     * @param propertyDescriptorName used to lookup if any property descriptors exist for that name
     * @return {@link PropertyDescriptor}
     */
    @Override
    public PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        Map<String, Object> kvStore;
        String prefix = getProperty(KV_PREFIX).getValue();
        try {
            kvStore = read(prefix);
        } catch (IOException e) {
            return null;
        }

        if (kvStore == null) {
            return null;
        }

        if (kvStore.containsKey(propertyDescriptorName))
            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .displayName(propertyDescriptorName)
                    .addValidator(Validator.VALID)
                    .dynamic(true)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .build();

        return null;
    }

    /**
     * This method will be called only when it has been determined that all
     * property values are valid according to their corresponding
     * PropertyDescriptor's validators.
     *
     * @param validationContext provides a mechanism for obtaining externally
     * managed values, such as property values and supplies convenience methods
     * for operating on those values
     *
     * @return Collection of ValidationResult objects that will be added to any
     * other validation findings - may be null
     */
    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>();

        final Function<PropertyDescriptor, Boolean> isNotSet = (prop) -> !validationContext.getProperty(prop).isSet();
        final Function<PropertyDescriptor, String> getValue = (prop) -> validationContext.getProperty(prop).getValue();
        final Function<PropertyDescriptor, ValidationResult.Builder> invalidResult = (prop) -> new ValidationResult.Builder().valid(false).subject(prop.getName());
        final Function<ValidationResult.Builder, Boolean> addResult = (builder) -> validationResults.add(builder.build());

        final SSLContextService sslContextService = validationContext.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final String mechanism = getValue.apply(USER_ID_MECHANISM);
        final String url = getValue.apply(URL);
        final URI uri;
        final String scheme;

        try {
            uri = new URI(url);
            scheme = uri.getScheme();

            // We require that the url is valid, and http or https:
            if (!scheme.equalsIgnoreCase("https") && !scheme.equalsIgnoreCase("http"))
                addResult.apply(invalidResult.apply(URL).input(url).explanation("Not a valid HTTP or HTTPS URL"));

            // We require an SSL context service with https urls:
            if (scheme.equalsIgnoreCase("https") && sslContextService == null)
                addResult.apply(invalidResult.apply(SSL_CONTEXT_SERVICE).explanation("SSL Context Service required for HTTPS URLs."));

        } catch (URISyntaxException e) {
            // This should have been caught by the parent validator, so this is basically making the compiler happy:
            addResult.apply(invalidResult.apply(URL).input(url).explanation("Not a valid URL"));
        }

        // We require that values related to the auth type are also valid:
        switch (getValue.apply(AUTH_TYPE)) {

            // For token auth, we require a token:
            case StandardVaultHttpClientConfiguration.AUTH_TYPE_TOKEN: {
                if (isNotSet.apply(TOKEN))
                    addResult.apply(invalidResult.apply(TOKEN).explanation("Token value required when configured for 'Token Authentication'."));
                break;
            }

            // For app id auth, we require that the app id is set, and that the user id is set (when the mechanism is static user id):
            case StandardVaultHttpClientConfiguration.AUTH_TYPE_APP_ID: {
                if (isNotSet.apply(APP_ID))
                    addResult.apply(invalidResult.apply(APP_ID).explanation("App Id required when configured for 'App Id Authentication'."));

                if (mechanism.equalsIgnoreCase(StandardVaultHttpClientConfiguration.APP_ID_STATIC_USER_ID) && isNotSet.apply(USER_ID))
                    addResult.apply(invalidResult.apply(USER_ID).explanation("User Id required when configured for 'App Id Authentication + Static User Id'."));
                break;
            }

            // For app role auth, we require both role id and user id fields:
            case StandardVaultHttpClientConfiguration.AUTH_TYPE_APP_ROLE: {
                if (isNotSet.apply(ROLE_ID))
                    addResult.apply(invalidResult.apply(ROLE_ID).explanation("Role Id required when configured for 'App Role Authentication'."));

                if (isNotSet.apply(SECRET_ID))
                    addResult.apply(invalidResult.apply(SECRET_ID).explanation("Secret Id required when configured for 'App Role Authentication'."));
                break;
            }

            // If or when new auth types are added, their properties should be validated here.

            // This can only happen if (or when) this validation code does not recognize a known good auth type:
            default:
                addResult.apply(invalidResult.apply(AUTH_TYPE).input(getValue.apply(AUTH_TYPE)).explanation("Auth type not handled."));
        }

        return validationResults;
    }

    /**
     * Provides the KV operations template for the current configuration of this instance.
     *
     * @return KV operations for this controller.
     */
    public VaultKeyValueOperations getKvOperations() {
        if (cachedServerOperations == null || cachedKvVersion == null || cachedKvOperations == null) {
            StandardVaultHttpClientConfiguration config = new StandardVaultHttpClientConfiguration(getConfigurationContext());
            String path = getProperty(SECRETS_PATH).getValue();

            cachedServerOperations = new VaultTemplate(config.vaultEndpoint(), config.createRequestFactory(), new SimpleSessionManager(config.clientAuthentication()));
            cachedKvVersion = getKvVersion(cachedServerOperations, path);

            if (cachedKvVersion != null)
                cachedKvOperations = cachedServerOperations.opsForKeyValue(path, cachedKvVersion);
        }
        return cachedKvOperations;
    }

    /**
     * Writes the contents of the given value to the server at the given path.
     *
     * @param path path to value,  "/" separated
     * @param value value to write, string, map, etc.
     */
    public void write(String path, Map<String, Object> value) throws IOException {
        VaultKeyValueOperations ops = getKvOperations();
        if (ops == null)
            throw new IOException("Cannot write to Vault - no KV operations available");

        ops.put(path, value);
        logger.info("KV write finished to {} wrote {} key(s)", path, value.keySet().size());
    }

    /**
     * Reads the map at the given path.
     *
     * @param path path to value,  "/" separated
     * @return contents at path, a map of string to object as returned by Vault
     * @throws IOException when path cannot be read
     */
    public Map<String, Object> read(String path) throws IOException {
        VaultKeyValueOperations ops = getKvOperations();
        if (ops == null)
            throw new IOException("Cannot read from Vault - no KV operations available");

        VaultResponse readResponse = ops.get(path);
        if (readResponse == null) {
            logger.info("KV null at path {}", path);
            return null;
        }

        Map<String, Object> dataResponse = readResponse.getData();
        logger.info("KV read finished from path {}", path);
        return dataResponse;
    }

    /**
     * Reads the values at a path, as a list.
     *
     * @param path path to values,  "/" separated
     * @return contents at path, a map of string to object as returned by Vault
     * @throws IOException when path cannot be listed
     */
    public Map<String, Object> list(String path) throws IOException {
        VaultKeyValueOperations ops = getKvOperations();
        if (ops == null)
            throw new IOException("Cannot list path in Vault - no KV operations available");

        Map<String, Object> others = new HashMap<>();
        for (String key : ops.list(path)) {
            Map<String, Object> value = read(path + "/" + key);
            others.put(key, value);
        }

        return others;
    }

    /**
     * Queries and returns the backend version (KV_1 or KV_2), or null if the version isn't 1 or 2.
     *
     * @param template Vault operations template
     * @param path path to the KV secrets engine
     * @return {@link VaultKeyValueOperationsSupport.KeyValueBackend} or null if KV engine not supported at path
     */
    private VaultKeyValueOperationsSupport.KeyValueBackend getKvVersion(VaultTemplate template, String path) {
        VaultMount mount = template.opsForSys().getMounts().get(path + "/");
        if (mount != null) {
            if (!mount.getType().equals("kv")) {
                logger.warn("Vault mount '{}' is not a KV store.", path);
                return null;
            }
            switch (Integer.parseInt(mount.getOptions().get("version"))) {
                case 1:
                    return VaultKeyValueOperationsSupport.KeyValueBackend.KV_1;
                case 2:
                    return VaultKeyValueOperationsSupport.KeyValueBackend.KV_2;
            }
        }
        logger.warn("KV '{}' version not recognized.", path);
        return null;
    }
}
