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
package org.apache.nifi.hazelcast.services.cachemanager;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Tags({"hazelcast", "cache"})
@CapabilityDescription("A service that provides cache instances backed by Hazelcast running outside of NiFi.")
public class StandaloneHazelcastCacheManager extends IMapBasedHazelcastCacheManager {

    /**
     * Used to involve some fluctuation into the backoff time. For details, please see Hazelcast documentation.
     */
    private static final double BACKOFF_JITTER = 0.2;

    public static final PropertyDescriptor HAZELCAST_SERVER_ADDRESS = new PropertyDescriptor.Builder()
            .name("hazelcast-server-address")
            .displayName("Hazelcast Server Address")
            .description("Address of the Hazelcast instance, using {host:port} format. In case there are multiple instances," +
                    " separate the instances using " + ADDRESS_SEPARATOR + ".")
            .required(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor HAZELCAST_RETRY_BACKOFF_INITIAL = new PropertyDescriptor.Builder()
            .name("hazelcast-retry-backoff-initial")
            .displayName("Hazelcast Initial Backoff")
            .description("The amount of time the client waits until it tries to reestablish connection at the first time.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .defaultValue("1 secs")
            .build();

    public static final PropertyDescriptor HAZELCAST_RETRY_BACKOFF_MAXIMUM = new PropertyDescriptor.Builder()
            .name("hazelcast-retry-backoff-maximum")
            .displayName("Hazelcast Maximum Backoff")
            .description("The longest amount of time the client waits until it tries to reestablish connection.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .defaultValue("5 secs")
            .build();

    public static final PropertyDescriptor HAZELCAST_RETRY_BACKOFF_MULTIPLIER = new PropertyDescriptor.Builder()
            .name("hazelcast-retry-backoff-multiplier")
            .displayName("Hazelcast Backoff Multiplier")
            .description("The multiplier the client uses to increase wait time for retries.")
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .required(true)
            .defaultValue("1.5")
            .build();

    public static final PropertyDescriptor HAZELCAST_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("hazelcast-connection-timeout")
            .displayName("Hazelcast Connection Timeout")
            .description("The maximum amount of time the client tries to connect or reconnect before abandon.")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .defaultValue("20 secs")
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HAZELCAST_CLUSTER_NAME);
        properties.add(HAZELCAST_SERVER_ADDRESS);
        properties.add(HAZELCAST_RETRY_BACKOFF_INITIAL);
        properties.add(HAZELCAST_RETRY_BACKOFF_MAXIMUM);
        properties.add(HAZELCAST_RETRY_BACKOFF_MULTIPLIER);
        properties.add(HAZELCAST_CONNECTION_TIMEOUT);
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(properties);
    }

    @Override
    protected HazelcastInstance getInstance(final ConfigurationContext context) {
        final ClientConfig config = new ClientConfig();

        if (context.getProperty(HAZELCAST_CLUSTER_NAME).isSet()) {
            config.setClusterName(context.getProperty(HAZELCAST_CLUSTER_NAME).evaluateAttributeExpressions().getValue());
        }

        config.getNetworkConfig()
                .addAddress(context.getProperty(HAZELCAST_SERVER_ADDRESS).evaluateAttributeExpressions().getValue().split(ADDRESS_SEPARATOR));

        config.getConnectionStrategyConfig().getConnectionRetryConfig()
            .setClusterConnectTimeoutMillis(context.getProperty(HAZELCAST_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).longValue())
            .setInitialBackoffMillis(context.getProperty(HAZELCAST_RETRY_BACKOFF_INITIAL).asTimePeriod(TimeUnit.MILLISECONDS).intValue())
            .setMaxBackoffMillis(context.getProperty(HAZELCAST_RETRY_BACKOFF_MAXIMUM).asTimePeriod(TimeUnit.MILLISECONDS).intValue())
            .setMultiplier(context.getProperty(HAZELCAST_RETRY_BACKOFF_MULTIPLIER).asDouble())
            .setJitter(BACKOFF_JITTER);

        return HazelcastClient.newHazelcastClient(config);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Set<ValidationResult> results = new HashSet<>();

        if (context.getProperty(HAZELCAST_RETRY_BACKOFF_INITIAL).asTimePeriod(TimeUnit.MILLISECONDS).compareTo((long) Integer.MAX_VALUE) > 0) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_RETRY_BACKOFF_INITIAL.getName())
                    .valid(false)
                    .explanation("Millisecond representation of initial backoff time must not be above " + Integer.MAX_VALUE + "!")
                    .build());
        }

        if (context.getProperty(HAZELCAST_RETRY_BACKOFF_MAXIMUM).asTimePeriod(TimeUnit.MILLISECONDS).compareTo((long) Integer.MAX_VALUE) > 0) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_RETRY_BACKOFF_MAXIMUM.getName())
                    .valid(false)
                    .explanation("Millisecond representation of maximum backoff time must not be above " + Integer.MAX_VALUE + "!")
                    .build());
        }

        return results;
    }
}
