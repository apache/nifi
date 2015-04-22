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
package org.apache.nifi.cluster.flow;

import java.util.Set;

import org.apache.nifi.cluster.protocol.NodeIdentifier;

/**
 * A service for managing the cluster's flow. The service will attempt to keep
 * the cluster's dataflow current while respecting the value of the configured
 * retrieval delay.
 *
 * The eligible retrieval time is reset with the configured delay every time the
 * flow state is set to STALE. If the state is set to UNKNOWN or CURRENT, then
 * the flow will not be retrieved.
 *
 * Clients must call start() and stop() to initialize and stop the instance.
 *
 */
public interface DataFlowManagementService {

    /**
     * Starts the instance. Start may only be called if the instance is not
     * running.
     */
    void start();

    /**
     * Stops the instance. Stop may only be called if the instance is running.
     */
    void stop();

    /**
     * @return true if the instance is started; false otherwise.
     */
    boolean isRunning();

    /**
     * Loads the dataflow.
     *
     * @return the dataflow or null if no dataflow exists
     */
    ClusterDataFlow loadDataFlow();

    /**
     * Updates the dataflow with the given primary node identifier.
     *
     * @param nodeId the node identifier
     *
     * @throws DaoException if the update failed
     */
    void updatePrimaryNode(NodeIdentifier nodeId) throws DaoException;

    /**
     * Updates the dataflow with the given serialized form of the Controller
     * Services that are to exist on the NCM.
     *
     * @param serializedControllerServices services
     * @throws DaoException ex
     */
    void updateControllerServices(byte[] serializedControllerServices) throws DaoException;

    /**
     * Updates the dataflow with the given serialized form of Reporting Tasks
     * that are to exist on the NCM.
     *
     * @param serializedReportingTasks tasks
     * @throws DaoException ex
     */
    void updateReportingTasks(byte[] serializedReportingTasks) throws DaoException;

    /**
     * Sets the state of the flow.
     *
     * @param flowState the state
     *
     * @see PersistedFlowState
     */
    void setPersistedFlowState(PersistedFlowState flowState);

    /**
     * @return the state of the flow
     */
    PersistedFlowState getPersistedFlowState();

    /**
     * @return true if the flow is current; false otherwise.
     */
    boolean isFlowCurrent();

    /**
     * Sets the node identifiers to use when attempting to retrieve the flow.
     *
     * @param nodeIds the node identifiers
     */
    void setNodeIds(Set<NodeIdentifier> nodeIds);

    /**
     * Returns the set of node identifiers the service is using to retrieve the
     * flow.
     *
     * @return the set of node identifiers the service is using to retrieve the
     * flow.
     */
    Set<NodeIdentifier> getNodeIds();

    /**
     * @return the retrieval delay in seconds
     */
    int getRetrievalDelaySeconds();

    /**
     * Sets the retrieval delay.
     *
     * @param delay the retrieval delay in seconds
     */
    void setRetrievalDelay(String delay);
}
