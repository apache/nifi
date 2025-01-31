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
package org.apache.nifi.redis.state;

import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProvider;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.redis.RedisType;
import org.apache.nifi.redis.util.RedisAction;
import org.apache.nifi.redis.util.RedisUtils;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A StateProvider backed by Redis.
 */
public class RedisStateProvider extends AbstractConfigurableComponent implements StateProvider {

    static final int ENCODING_VERSION = 1;

    public static final PropertyDescriptor KEY_PREFIX = new PropertyDescriptor.Builder()
            .name("Key Prefix")
            .displayName("Key Prefix")
            .description("The prefix for each key stored by this state provider. When sharing a single Redis across multiple NiFi instances, " +
                    "setting a unique value for the Key Prefix will make it easier to identify which instances the keys came from.")
            .required(true)
            .defaultValue("nifi/components/")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    public static final PropertyDescriptor ENABLE_TLS = new PropertyDescriptor.Builder()
            .name("Enable TLS")
            .displayName("Enable TLS")
            .description("If true, the Redis connection will be configured to use TLS, using the keystore and truststore settings configured in " +
                    "nifi.properties.  This means that a TLS-enabled Redis connection is only possible if the Apache NiFi instance is running in secure mode. " +
                    "If this property is false, an insecure Redis connection will be used even if the Apache NiFi instance is secure.")
            .required(true)
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();
    public static final PropertyDescriptor MAX_ATTEMPTS = new PropertyDescriptor.Builder()
            .name("Max Attempts")
            .displayName("Max Attempts")
            .description("Maximum number of attempts when setting/clearing the state for a component. This number should be higher than the number of nodes "
                    + "in the NiFi cluster to account for the case where each node may concurrently try to clear a state with a local scope.")
            .required(true)
            .defaultValue("20")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final List<PropertyDescriptor> STATE_PROVIDER_PROPERTIES;
    static {
        final List<PropertyDescriptor> props = new ArrayList<>(RedisUtils.REDIS_CONNECTION_PROPERTY_DESCRIPTORS);
        props.add(KEY_PREFIX);
        props.add(ENABLE_TLS);
        props.add(MAX_ATTEMPTS);
        STATE_PROVIDER_PROPERTIES = Collections.unmodifiableList(props);
    }

    private static final String KEY_PATTERN_FORMAT = "%s*";

    private static final String KEY_PREFIX_COMPONENT_ID_PATTERN = "^%s(.+)$";

    private static final int COMPONENT_ID_GROUP = 1;

    private String identifier;
    private String keyPrefix;
    private int maxAttempts;
    private ComponentLog logger;
    private PropertyContext context;
    private SSLContext sslContext;

    private volatile boolean enabled;
    private volatile JedisConnectionFactory connectionFactory;

    private final RedisStateMapSerDe serDe = new RedisStateMapJsonSerDe();

    @Override
    public final void initialize(final StateProviderInitializationContext context) {
        this.context = context;
        if (context.getProperty(ENABLE_TLS).asBoolean()) {
            this.sslContext = context.getSSLContext();
        }
        this.identifier = context.getIdentifier();
        this.logger = context.getLogger();

        String keyPrefix = context.getProperty(KEY_PREFIX).getValue();
        if (!keyPrefix.endsWith("/")) {
            keyPrefix = keyPrefix + "/";
        }
        this.keyPrefix = keyPrefix;

        this.maxAttempts = context.getProperty(MAX_ATTEMPTS).asInteger();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return STATE_PROVIDER_PROPERTIES;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(RedisUtils.validate(validationContext));

        final RedisType redisType = RedisType.fromDisplayName(validationContext.getProperty(RedisUtils.REDIS_MODE).getValue());
        if (redisType == RedisType.CLUSTER) {
            results.add(new ValidationResult.Builder()
                    .subject(RedisUtils.REDIS_MODE.getDisplayName())
                    .valid(false)
                    .explanation(RedisUtils.REDIS_MODE.getDisplayName()
                            + " is configured in clustered mode, and this service requires a non-clustered Redis")
                    .build());
        }
        final boolean enableTls = validationContext.getProperty(ENABLE_TLS).asBoolean();
        if (enableTls && sslContext == null) {
            results.add(new ValidationResult.Builder()
                    .subject(ENABLE_TLS.getDisplayName())
                    .valid(false)
                    .explanation(ENABLE_TLS.getDisplayName()
                            + " is set to 'true', but Apache NiFi is not secured.  This state provider can only use a TLS-enabled connection " +
                            "if a keystore and truststore are provided in nifi.properties.")
                    .build());
        }

        return results;
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public void enable() {
        enabled = true;
    }

    @Override
    public void disable() {
        enabled = false;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void shutdown() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
            connectionFactory = null;
        }
    }

    @Override
    public void setState(final Map<String, String> state, final String componentId) throws IOException {
        verifyEnabled();

        final StateMap currStateMap = getState(componentId);

        int attempted = 0;
        boolean updated = false;

        while (!updated && attempted < this.maxAttempts) {
            updated = replace(currStateMap, state, componentId);
            attempted++;
        }

        if (!updated) {
            throw new IOException("Unable to update state due to concurrent modifications");
        }
    }

    @Override
    public StateMap getState(final String componentId) throws IOException {
        return withConnection(redisConnection -> {
            final byte[] key = getComponentKey(componentId).getBytes(StandardCharsets.UTF_8);
            final byte[] value = redisConnection.stringCommands().get(key);

            final RedisStateMap stateMap = serDe.deserialize(value);
            if (stateMap == null) {
                return new RedisStateMap.Builder().encodingVersion(ENCODING_VERSION).build();
            } else {
                return stateMap;
            }
        });
    }

    @Override
    public boolean replace(final StateMap oldValue, final Map<String, String> newValue, final String componentId) throws IOException {
        return withConnection(redisConnection -> {

            boolean replaced = false;

            // start a watch on the key and retrieve the current value
            final byte[] key = getComponentKey(componentId).getBytes(StandardCharsets.UTF_8);
            redisConnection.watch(key);

            final Optional<String> previousVersion = oldValue == null ? Optional.empty() : oldValue.getStateVersion();

            final byte[] currValue = redisConnection.stringCommands().get(key);
            final RedisStateMap currStateMap = serDe.deserialize(currValue);
            final Optional<String> currentVersion = currStateMap == null ? Optional.empty() : currStateMap.getStateVersion();

            // start a transaction
            redisConnection.multi();

            // compare-and-set
            if (previousVersion.equals(currentVersion)) {
                // build the new RedisStateMap incrementing the version, using latest encoding, and using the passed in values
                final long currentVersionNumber = currentVersion.map(Long::parseLong).orElse(RedisStateMapJsonSerDe.EMPTY_VERSION);
                final RedisStateMap newStateMap = new RedisStateMap.Builder()
                        .version(currentVersionNumber + 1)
                        .encodingVersion(ENCODING_VERSION)
                        .stateValues(newValue)
                        .build();

                // if we use set(k, newVal) then the results list will always have size == 0 b/c when convertPipelineAndTxResults is set to true,
                // status responses like "OK" are skipped over, so by using getSet we can rely on the results list to know if the transaction succeeded
                redisConnection.stringCommands().getSet(key, serDe.serialize(newStateMap));
            }

            // execute the transaction
            final List<Object> results = redisConnection.exec();

            // if we have a result then the replace succeeded
            // results can be null if the transaction has been aborted
            if (results != null && results.size() > 0) {
                replaced = true;
            }

            return replaced;
        });
    }

    @Override
    public void clear(final String componentId) throws IOException {
        int attempted = 0;
        boolean updated = false;

        while (!updated && attempted < this.maxAttempts) {
            final StateMap currStateMap = getState(componentId);
            updated = replace(currStateMap, Collections.emptyMap(), componentId);

            final String result = updated ? "successful" : "unsuccessful";
            logger.debug("Attempt # {} to clear state for component {} was {}", attempted + 1, componentId, result);

            attempted++;
        }

        if (!updated) {
            throw new IOException("Unable to update state due to concurrent modifications");
        }
    }

    @Override
    public void onComponentRemoved(final String componentId) throws IOException {
        withConnection(redisConnection -> {
            final byte[] key = getComponentKey(componentId).getBytes(StandardCharsets.UTF_8);
            redisConnection.keyCommands().del(key);
            return true;
        });
    }

    @Override
    public Scope[] getSupportedScopes() {
        return new Scope[] {Scope.CLUSTER};
    }

    @Override
    public boolean isComponentEnumerationSupported() {
        return true;
    }

    /**
     * Get Component Identifiers with stored state based on Redis Keys matching key prefix pattern
     *
     * @return Component Identifiers with stored state or empty when none found
     * @throws IOException Thrown on Redis communication failures
     */
    @Override
    public Collection<String> getStoredComponentIds() throws IOException {
        final byte[] keyPattern = String.format(KEY_PATTERN_FORMAT, keyPrefix).getBytes(StandardCharsets.UTF_8);
        final Pattern keyPrefixComponentIdPattern = Pattern.compile(String.format(KEY_PREFIX_COMPONENT_ID_PATTERN, keyPrefix));

        return withConnection(redisConnection -> {
            final Set<byte[]> keys = redisConnection.keyCommands().keys(keyPattern);
            final Set<byte[]> keysFound = keys == null ? Collections.emptySet() : keys;
            return keysFound.stream()
                    .map(key -> new String(key, StandardCharsets.UTF_8))
                    .map(keyPrefixComponentIdPattern::matcher)
                    .filter(Matcher::matches)
                    .map(matcher -> matcher.group(COMPONENT_ID_GROUP))
                    .collect(Collectors.toUnmodifiableList());
        });
    }

    private String getComponentKey(final String componentId) {
        return keyPrefix + componentId;
    }

    private void verifyEnabled() throws IOException {
        if (!isEnabled()) {
            throw new IOException("Cannot update or retrieve cluster state because node is no longer connected to a cluster.");
        }
    }

    // visible for testing
    synchronized RedisConnection getRedis() {
        if (connectionFactory == null) {
            connectionFactory = RedisUtils.createConnectionFactory(context, sslContext);
        }

        return connectionFactory.getConnection();
    }

    private <T> T withConnection(final RedisAction<T> action) throws IOException {
        RedisConnection redisConnection = null;
        try {
            redisConnection = getRedis();
            return action.execute(redisConnection);
        } finally {
            if (redisConnection != null) {
                try {
                    redisConnection.close();
                } catch (Exception e) {
                    logger.warn("Error closing connection", e);
                }
            }
        }
    }

}
