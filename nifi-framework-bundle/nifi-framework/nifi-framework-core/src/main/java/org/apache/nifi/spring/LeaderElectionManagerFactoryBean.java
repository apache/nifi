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
package org.apache.nifi.spring;

import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import java.util.Set;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.leader.election.StandaloneLeaderElectionManager;
import org.apache.nifi.nar.ExtensionDefinition;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.factory.FactoryBean;

import static org.apache.nifi.util.NiFiProperties.CLUSTER_LEADER_ELECTION_IMPLEMENTATION;
import static org.apache.nifi.util.NiFiProperties.DEFAULT_CLUSTER_LEADER_ELECTION_IMPLEMENTATION;

public class LeaderElectionManagerFactoryBean implements FactoryBean<LeaderElectionManager> {
    private ExtensionManager extensionManager;

    private NiFiProperties properties;

    @Override
    public LeaderElectionManager getObject() throws Exception {
        final boolean isNode = properties.isNode();
        if (isNode) {
            return loadClusterLeaderElectionManager();
        } else {
            return new StandaloneLeaderElectionManager();
        }
    }

    @Override
    public Class<?> getObjectType() {
        return LeaderElectionManager.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setExtensionManager(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    private LeaderElectionManager loadClusterLeaderElectionManager() throws ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        final String leaderElectionImplementation = properties.getProperty(CLUSTER_LEADER_ELECTION_IMPLEMENTATION, DEFAULT_CLUSTER_LEADER_ELECTION_IMPLEMENTATION);
        final Set<ExtensionDefinition> extensions = extensionManager.getExtensions(LeaderElectionManager.class);
        final Optional<ExtensionDefinition> extensionFound = extensions.stream()
                .filter(extensionDefinition -> {
                    final String extensionClassName = extensionDefinition.getImplementationClassName();
                    return extensionClassName.equals(leaderElectionImplementation) || extensionClassName.endsWith(leaderElectionImplementation);
                })
                .findFirst();
        final ExtensionDefinition extension = extensionFound.orElseThrow(() -> {
            final String message = String.format("No Extensions Found for %s", LeaderElectionManager.class.getName());
            return new IllegalStateException(message);
        });
        final String extensionImplementationClass = extension.getImplementationClassName();

        return NarThreadContextClassLoader.createInstance(extensionManager, extensionImplementationClass, LeaderElectionManager.class, properties);
    }
}
