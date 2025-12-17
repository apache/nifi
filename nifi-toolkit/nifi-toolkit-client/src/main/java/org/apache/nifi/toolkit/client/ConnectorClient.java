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
package org.apache.nifi.toolkit.client;

import org.apache.nifi.web.api.entity.ConfigurationStepEntity;
import org.apache.nifi.web.api.entity.ConfigurationStepNamesEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ConnectorPropertyAllowableValuesEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;
import org.apache.nifi.web.api.entity.VerifyConnectorConfigStepRequestEntity;

import java.io.IOException;

/**
 * Client for interacting with the Connector REST endpoints.
 */
public interface ConnectorClient {

    /**
     * Creates a new connector.
     *
     * @param connectorEntity the connector entity to create
     * @return the created connector entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConnectorEntity createConnector(ConnectorEntity connectorEntity) throws NiFiClientException, IOException;

    /**
     * Gets a connector by ID.
     *
     * @param connectorId the connector ID
     * @return the connector entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConnectorEntity getConnector(String connectorId) throws NiFiClientException, IOException;

    /**
     * Updates a connector.
     *
     * @param connectorEntity the connector entity with updates
     * @return the updated connector entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConnectorEntity updateConnector(ConnectorEntity connectorEntity) throws NiFiClientException, IOException;

    /**
     * Deletes a connector.
     *
     * @param connectorId the connector ID
     * @param clientId the client ID
     * @param version the revision version
     * @return the deleted connector entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConnectorEntity deleteConnector(String connectorId, String clientId, long version) throws NiFiClientException, IOException;

    /**
     * Deletes a connector using the information from the entity.
     *
     * @param connectorEntity the connector entity to delete
     * @return the deleted connector entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConnectorEntity deleteConnector(ConnectorEntity connectorEntity) throws NiFiClientException, IOException;

    /**
     * Starts a connector.
     *
     * @param connectorId the connector ID
     * @param clientId the client ID
     * @param version the revision version
     * @return the updated connector entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConnectorEntity startConnector(String connectorId, String clientId, long version) throws NiFiClientException, IOException;

    /**
     * Starts a connector using the information from the entity.
     *
     * @param connectorEntity the connector entity to start
     * @return the updated connector entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConnectorEntity startConnector(ConnectorEntity connectorEntity) throws NiFiClientException, IOException;

    /**
     * Stops a connector.
     *
     * @param connectorId the connector ID
     * @param clientId the client ID
     * @param version the revision version
     * @return the updated connector entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConnectorEntity stopConnector(String connectorId, String clientId, long version) throws NiFiClientException, IOException;

    /**
     * Stops a connector using the information from the entity.
     *
     * @param connectorEntity the connector entity to stop
     * @return the updated connector entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConnectorEntity stopConnector(ConnectorEntity connectorEntity) throws NiFiClientException, IOException;

    /**
     * Gets the configuration step names for a connector.
     *
     * @param connectorId the connector ID
     * @return the configuration step names entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConfigurationStepNamesEntity getConfigurationSteps(String connectorId) throws NiFiClientException, IOException;

    /**
     * Gets a specific configuration step for a connector.
     *
     * @param connectorId the connector ID
     * @param configurationStepName the configuration step name
     * @return the configuration step entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConfigurationStepEntity getConfigurationStep(String connectorId, String configurationStepName) throws NiFiClientException, IOException;

    /**
     * Gets the allowable values for a property in a configuration step.
     *
     * @param connectorId the connector ID
     * @param configurationStepName the configuration step name
     * @param propertyGroupName the property group name
     * @param propertyName the property name
     * @param filter optional filter for the allowable values
     * @return the allowable values entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConnectorPropertyAllowableValuesEntity getPropertyAllowableValues(String connectorId, String configurationStepName,
            String propertyGroupName, String propertyName, String filter) throws NiFiClientException, IOException;

    /**
     * Updates a configuration step for a connector.
     *
     * @param configurationStepEntity the configuration step entity with updates
     * @return the updated configuration step entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConfigurationStepEntity updateConfigurationStep(ConfigurationStepEntity configurationStepEntity) throws NiFiClientException, IOException;

    /**
     * Submits a configuration step verification request.
     *
     * @param requestEntity the verification request entity
     * @return the verification request entity with status
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    VerifyConnectorConfigStepRequestEntity submitConfigStepVerificationRequest(VerifyConnectorConfigStepRequestEntity requestEntity) throws NiFiClientException, IOException;

    /**
     * Gets a configuration step verification request.
     *
     * @param connectorId the connector ID
     * @param configurationStepName the configuration step name
     * @param requestId the verification request ID
     * @return the verification request entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    VerifyConnectorConfigStepRequestEntity getConfigStepVerificationRequest(String connectorId, String configurationStepName,
            String requestId) throws NiFiClientException, IOException;

    /**
     * Deletes a configuration step verification request.
     *
     * @param connectorId the connector ID
     * @param configurationStepName the configuration step name
     * @param requestId the verification request ID
     * @return the deleted verification request entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    VerifyConnectorConfigStepRequestEntity deleteConfigStepVerificationRequest(String connectorId, String configurationStepName,
            String requestId) throws NiFiClientException, IOException;

    /**
     * Applies an update to a connector.
     *
     * @param connectorEntity the connector entity with revision
     * @return the updated connector entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ConnectorEntity applyUpdate(ConnectorEntity connectorEntity) throws NiFiClientException, IOException;

    /**
     * Gets the flow for the process group managed by a connector.
     *
     * @param connectorId the connector ID
     * @return the process group flow entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ProcessGroupFlowEntity getFlow(String connectorId) throws NiFiClientException, IOException;

    /**
     * Gets the flow for the process group managed by a connector.
     *
     * @param connectorId the connector ID
     * @param uiOnly whether to return only UI-specific fields
     * @return the process group flow entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ProcessGroupFlowEntity getFlow(String connectorId, boolean uiOnly) throws NiFiClientException, IOException;

    /**
     * Gets the status for the process group managed by a connector.
     *
     * @param connectorId the connector ID
     * @param recursive whether to include status for all descendant components
     * @return the process group status entity
     * @throws NiFiClientException if an error occurs during the request
     * @throws IOException if an I/O error occurs
     */
    ProcessGroupStatusEntity getStatus(String connectorId, boolean recursive) throws NiFiClientException, IOException;

    /**
     * Indicates that mutable requests should indicate that the client has
     * acknowledged that the node is disconnected.
     */
    void acknowledgeDisconnectedNode();
}

