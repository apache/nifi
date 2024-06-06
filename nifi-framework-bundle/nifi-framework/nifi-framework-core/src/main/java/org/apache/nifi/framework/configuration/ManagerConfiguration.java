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
package org.apache.nifi.framework.configuration;

import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.leader.election.StandaloneLeaderElectionManager;
import org.apache.nifi.controller.state.manager.StandardStateManagerProvider;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.events.StandardEventReporter;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.ExtensionManagerHolder;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.parameter.ParameterLookup;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.Set;

import static org.apache.nifi.util.NiFiProperties.CLUSTER_LEADER_ELECTION_IMPLEMENTATION;
import static org.apache.nifi.util.NiFiProperties.DEFAULT_CLUSTER_LEADER_ELECTION_IMPLEMENTATION;

/**
 * Shared Manager Configuration distinct from Flow Controller Configuration to avoid dependency resolution issues
 */
@Configuration
public class ManagerConfiguration {
    private NiFiProperties properties;

    private SSLContext sslContext;

    @Autowired
    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    @Autowired(required = false)
    public void setSslContext(final SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    /**
     * Extension Manager for tracking components
     *
     * @return Extension Discovering Manager
     */
    @Bean
    public ExtensionDiscoveringManager extensionManager() {
        return (ExtensionDiscoveringManager) ExtensionManagerHolder.getExtensionManager();
    }

    /**
     * State Manager Provider based on local and clustered state management configuration
     *
     * @return State Manager Provider
     */
    @Bean
    public StateManagerProvider stateManagerProvider() {
        try {
            return StandardStateManagerProvider.create(properties, sslContext, extensionManager(), ParameterLookup.EMPTY);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to create State Manager Provider", e);
        }
    }

    /**
     * Leader Election Manager implementation based on cluster configuration
     *
     * @return Leader Election Manager
     * @throws Exception Thrown on failure to create Leader Election Manager
     */
    @Bean
    public LeaderElectionManager leaderElectionManager() throws Exception {
        final LeaderElectionManager leaderElectionManager;

        if (properties.isClustered()) {
            final ExtensionDiscoveringManager extensionManager = extensionManager();

            final String configuredClassName = properties.getProperty(CLUSTER_LEADER_ELECTION_IMPLEMENTATION, DEFAULT_CLUSTER_LEADER_ELECTION_IMPLEMENTATION);
            final Set<ExtensionDefinition> extensions = extensionManager.getExtensions(LeaderElectionManager.class);

            final Optional<ExtensionDefinition> extensionFound = extensions.stream()
                    .filter(extensionDefinition -> {
                        final String extensionClassName = extensionDefinition.getImplementationClassName();
                        return extensionClassName.equals(configuredClassName) || extensionClassName.endsWith(configuredClassName);
                    })
                    .findFirst();

            final ExtensionDefinition extension = extensionFound.orElseThrow(() -> {
                final String message = String.format("No Extensions Found for %s", LeaderElectionManager.class.getName());
                return new IllegalStateException(message);
            });
            final String extensionImplementationClass = extension.getImplementationClassName();

            leaderElectionManager = NarThreadContextClassLoader.createInstance(extensionManager, extensionImplementationClass, LeaderElectionManager.class, properties);
        } else {
            leaderElectionManager = new StandaloneLeaderElectionManager();
        }

        return leaderElectionManager;
    }

    /**
     * Bulletin Repository using volatile memory-based implementation
     *
     * @return Bulletin Repository
     */
    @Bean
    public BulletinRepository bulletinRepository() {
        return new VolatileBulletinRepository();
    }

    /**
     * Event Reporter requires Bulletin Repository
     *
     * @return Event Reporter
     */
    @Bean
    public EventReporter eventReporter() {
        return new StandardEventReporter(bulletinRepository());
    }
}
