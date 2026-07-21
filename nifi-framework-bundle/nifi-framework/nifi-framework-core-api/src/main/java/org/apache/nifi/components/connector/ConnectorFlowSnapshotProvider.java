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

import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;

/**
 * Supplies {@link RegisteredFlowSnapshot} instances for Process Groups that are being inspected as
 * candidate sources for Connector migration. Implementations delegate to the standard flow snapshot
 * facility so that the migration manager and the listing path use a single mechanism for snapshot
 * acquisition rather than reimplementing flow mapping in framework-core.
 */
@FunctionalInterface
public interface ConnectorFlowSnapshotProvider {

    /**
     * Returns the current flow snapshot for the Process Group with the given identifier, optionally
     * including referenced controller services and exported component state.
     *
     * @param processGroupId the identifier of the Process Group to snapshot
     * @param includeReferencedServices whether to include controller services referenced from outside the group
     * @param includeComponentState whether to include LOCAL and CLUSTER component state in the snapshot
     * @return the flow snapshot
     */
    RegisteredFlowSnapshot getCurrentFlowSnapshotByGroupId(String processGroupId, boolean includeReferencedServices, boolean includeComponentState);
}
