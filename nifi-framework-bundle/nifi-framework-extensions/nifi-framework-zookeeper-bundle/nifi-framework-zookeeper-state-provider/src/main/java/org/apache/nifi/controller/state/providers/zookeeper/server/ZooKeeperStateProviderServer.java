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
package org.apache.nifi.controller.state.providers.zookeeper.server;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.state.StateProviderInitializationContext;
import org.apache.nifi.components.state.annotation.StateProviderContext;
import org.apache.nifi.controller.state.providers.zookeeper.AbstractStateProvider;
import org.apache.nifi.util.NiFiProperties;

import java.util.Map;
import java.util.Objects;

/**
 * State Provider wrapper for Embedded ZooKeeper Server supporting abstracted access through framework FlowController
 */
public class ZooKeeperStateProviderServer extends AbstractStateProvider {
    private NiFiProperties properties;

    private ZooKeeperStateServer server;

    @StateProviderContext
    public void setProperties(final NiFiProperties properties) {
        this.properties = Objects.requireNonNull(properties, "Properties required");
    }

    @Override
    public void init(final StateProviderInitializationContext context) {

    }

    @Override
    public void enable() {
        super.enable();
        try {
            server = ZooKeeperStateServer.create(properties);
            server.start();
        } catch (final Exception e) {
            throw new IllegalStateException("Failed to start Embedded ZooKeeper Server", e);
        }
    }

    @Override
    public void shutdown() {
        if (server != null) {
            server.shutdown();
            server = null;
        }
    }

    @Override
    public void setState(final Map<String, String> state, final String componentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StateMap getState(final String componentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean replace(final StateMap oldValue, final Map<String, String> newValue, final String componentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear(final String componentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onComponentRemoved(final String componentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Scope[] getSupportedScopes() {
        return new Scope[0];
    }
}
