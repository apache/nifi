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

package org.apache.nifi.components.connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * <p>
 * Default implementation of {@link ConnectorRepository} that stores connector information
 * as part of the flow.json.gz file. This is the default behavior where connectors are
 * embedded within the overall NiFi flow configuration.
 * </p>
 *
 * <p>
 * With this implementation, connector persistence is handled by the flow serialization
 * mechanism. The {@link #save(PersistedConnectorRecord)} and {@link #delete(String)} methods
 * are effectively no-ops because the actual persistence happens through the flow save mechanism
 * triggered via the {@link ConnectorUpdateCallback}.
 * </p>
 */
public class FlowJsonConnectorRepository implements ConnectorRepository {
    private static final Logger logger = LoggerFactory.getLogger(FlowJsonConnectorRepository.class);

    @Override
    public void initialize(final ConnectorRepositoryContext context) throws IOException {
        logger.info("Initialized FlowJsonConnectorRepository - connector persistence embedded in flow.json.gz");
    }

    @Override
    public void save(final PersistedConnectorRecord record) throws IOException {
        // No-op: In the default implementation, connectors are saved as part of the flow.json.gz
        // The actual persistence happens when the flow is saved via ConnectorUpdateCallback.persist()
        logger.debug("save() called for connector {}; persistence handled via flow.json.gz", record.getIdentifier());
    }

    @Override
    public List<PersistedConnectorRecord> loadAll() throws IOException {
        // In the default implementation, connectors are loaded from flow.json.gz during flow synchronization
        // This method is not used for flow.json.gz-based persistence
        logger.debug("loadAll() called; connectors loaded from flow.json.gz during synchronization");
        return Collections.emptyList();
    }

    @Override
    public Optional<PersistedConnectorRecord> load(final String connectorId) throws IOException {
        // In the default implementation, connectors are loaded from flow.json.gz during flow synchronization
        // This method is not used for flow.json.gz-based persistence
        logger.debug("load() called for connector {}; connectors loaded from flow.json.gz during synchronization", connectorId);
        return Optional.empty();
    }

    @Override
    public void delete(final String connectorId) throws IOException {
        // No-op: In the default implementation, connectors are removed from the flow.json.gz
        // The actual persistence happens when the flow is saved via ConnectorUpdateCallback.persist()
        logger.debug("delete() called for connector {}; persistence handled via flow.json.gz", connectorId);
    }

    @Override
    public boolean exists(final String connectorId) {
        // In the default implementation, this would need access to FlowManager to check
        // But since connectors are managed through the flow, we return false and let
        // the FlowManager handle existence checks
        return false;
    }

    @Override
    public void shutdown() {
        logger.info("Shutting down FlowJsonConnectorRepository");
    }
}
