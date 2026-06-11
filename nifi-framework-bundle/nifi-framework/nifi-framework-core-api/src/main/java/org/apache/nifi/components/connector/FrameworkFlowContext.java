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

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;

public interface FrameworkFlowContext extends FlowContext {
    ProcessGroup getManagedProcessGroup();

    @Override
    MutableConnectorConfigurationContext getConfigurationContext();

    void updateFlow(VersionedExternalFlow versionedExternalFlow, AssetManager assetManager) throws FlowUpdateException;

    /**
     * Verifies that the Managed Process Group can be updated to the given {@link VersionedExternalFlow}, without
     * performing any modifications. This is a read-only preview of the checks performed at the start of
     * {@link #updateFlow(VersionedExternalFlow, AssetManager)} and is intended to let callers reject update
     * requests synchronously (e.g. during the REST verify-phase) before any state mutation occurs.
     *
     * @param versionedExternalFlow the proposed updated flow
     * @throws FlowUpdateException if the Managed Process Group is not in a state that will allow the update
     */
    void verifyUpdateFlow(VersionedExternalFlow versionedExternalFlow) throws FlowUpdateException;

    /**
     * Restores the contents of the Managed Process Group from a snapshot persisted while the Connector was in
     * Troubleshooting mode. Unlike {@link #updateFlow(VersionedExternalFlow, AssetManager)}, this method does
     * <em>not</em> call {@code ProcessGroup#verifyCanUpdate} and is intended for use only when restoring a Connector
     * that was persisted while in Troubleshooting mode. The persisted user modifications are re-applied on top of the
     * Connector's current Managed Process Group contents.
     *
     * @param troubleshootingProcessGroup the VersionedProcessGroup representing the Managed Process Group contents
     *                                    persisted while the Connector was in Troubleshooting mode
     */
    void restoreTroubleshootingFlow(VersionedProcessGroup troubleshootingProcessGroup);
}
