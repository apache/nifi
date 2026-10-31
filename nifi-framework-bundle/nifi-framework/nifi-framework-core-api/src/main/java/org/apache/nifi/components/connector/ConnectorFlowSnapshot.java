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

import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.Map;

/**
 * Authoritative Connector flow state serialized as one consistent unit.
 *
 * @param flow the managed flow as applied, never the live group while in troubleshooting; persisted as
 *             {@code VersionedConnector.managedProcessGroup}
 * @param parameterContext the managed parameter context of that flow, or null; persisted as
 *             {@code VersionedConnector.managedParameterContext}. Sensitive values are stripped.
 * @param parameterSecretReferences bindings from managed parameter names to configuration secret references; persisted
 *             as {@code VersionedConnector.managedParameterSecretReferences}
 * @param bundle the Connector bundle that built the flow; persisted as {@code VersionedConnector.managedFlowBundle}
 * @param activeConfiguration the migrated ACTIVE configuration persisted with this snapshot as
 *             {@code VersionedConnector.activeFlowConfiguration}
 */
public record ConnectorFlowSnapshot(
        VersionedProcessGroup flow,
        VersionedParameterContext parameterContext,
        Map<String, SecretReference> parameterSecretReferences,
        Bundle bundle,
        ConnectorConfiguration activeConfiguration) {

    public ConnectorFlowSnapshot {
        parameterSecretReferences = parameterSecretReferences == null ? Map.of() : Map.copyOf(parameterSecretReferences);
    }
}
