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

/**
 * Callback interface for reporting connector state changes during lifecycle operations.
 * This allows the {@link ConnectorLifecycleManager} to notify the framework of state transitions.
 */
public interface ConnectorStateCallback {

    /**
     * Called when a connector's state has changed.
     *
     * @param connectorId the identifier of the connector whose state changed
     * @param previousState the previous state of the connector
     * @param newState the new state of the connector
     */
    void onStateChange(String connectorId, ConnectorState previousState, ConnectorState newState);

    /**
     * Called when a lifecycle operation completes successfully.
     *
     * @param connectorId the identifier of the connector
     * @param operation a description of the operation that completed
     */
    void onOperationComplete(String connectorId, String operation);

    /**
     * Called when a lifecycle operation fails.
     *
     * @param connectorId the identifier of the connector
     * @param operation a description of the operation that failed
     * @param cause the cause of the failure
     */
    void onOperationFailed(String connectorId, String operation, Throwable cause);
}
