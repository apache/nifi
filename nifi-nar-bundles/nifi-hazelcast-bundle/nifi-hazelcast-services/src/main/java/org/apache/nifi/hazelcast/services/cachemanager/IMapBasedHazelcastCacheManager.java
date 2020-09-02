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
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.hazelcast.services.cache.HazelcastCache;
import org.apache.nifi.hazelcast.services.cache.IMapBasedHazelcastCache;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.net.BindException;
import java.util.List;

abstract class IMapBasedHazelcastCacheManager extends AbstractControllerService implements HazelcastCacheManager {
    protected static final String ADDRESS_SEPARATOR = ",";

    /**
     * Used to involve some fluctuation into the backoff time. For details, please see Hazelcast documentation.
     */
    protected static final double CLIENT_BACKOFF_JITTER = 0.2;
    protected static final long DEFAULT_CLIENT_TIMEOUT_MAXIMUM_IN_SEC = 20;
    protected static final long DEFAULT_CLIENT_BACKOFF_INITIAL_IN_SEC = 1;
    protected static final long DEFAULT_CLIENT_BACKOFF_MAXIMUM_IN_SEC = 5;
    protected static final double DEFAULT_CLIENT_BACKOFF_MULTIPLIER = 1.5;

    public static final PropertyDescriptor HAZELCAST_CLUSTER_NAME = new PropertyDescriptor.Builder()
            .name("hazelcast-cluster-name")
            .displayName("Hazelcast Cluster Name")
            .description("Name of the Hazelcast cluster.")
            .defaultValue("nifi") // Hazelcast's default is "dev", "nifi" overwrites this.
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private volatile HazelcastInstance instance;

    @Override
    public HazelcastCache getCache(final String name, final long ttlInMillis) {
        return new IMapBasedHazelcastCache(instance.getMap(name), ttlInMillis);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        try {
            instance = getInstance(context);
        } catch (final Exception e) {
            getLogger().error("Could not create Hazelcast instance. Reason: " + e.getMessage(), e);

            // In case of bind exception, we provide a more specific error message to avoid ambiguity
            if (e.getCause() instanceof BindException && e.getCause().getMessage().equals("Address already in use")) {
                throw new InitializationException("The given port is already in use, probably by an externally running Hazelcast instance!");
            } else {
                throw new InitializationException(e);
            }
        }
    }

    @OnShutdown
    @OnDisabled
    public void shutdown() {
        if (instance != null) {
            instance.shutdown();
            instance = null;
        }
    }

    protected HazelcastInstance getClientInstance(
            final String clusterName,
            final List<String> serverAddresses,
            final long maxTimeout,
            final int initialBackoff,
            final int maxBackoff,
            final double backoffMultiplier) {
        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName(clusterName);
        clientConfig.getNetworkConfig().setAddresses(serverAddresses);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig()
                .setClusterConnectTimeoutMillis(maxTimeout)
                .setInitialBackoffMillis(initialBackoff)
                .setMaxBackoffMillis(maxBackoff)
                .setMultiplier(backoffMultiplier)
                .setJitter(CLIENT_BACKOFF_JITTER);

        return HazelcastClient.newHazelcastClient(clientConfig);
    }

    protected abstract HazelcastInstance getInstance(final ConfigurationContext context);
}
