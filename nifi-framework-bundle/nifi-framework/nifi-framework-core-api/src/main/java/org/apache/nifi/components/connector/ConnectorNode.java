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

import org.apache.nifi.authorization.resource.ComponentAuthorizable;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.VersionedComponent;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.FlowFileTransferCounts;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConfigurationStep;
import org.apache.nifi.logging.ComponentLog;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;

public interface ConnectorNode extends ComponentAuthorizable, VersionedComponent {

    String getName();

    ConnectorState getCurrentState();

    ConnectorState getDesiredState();

    /**
     * Verifies that the Connector is in a state that allows it to be deleted. If the Connector is not in a state
     * that allows it to be deleted, this method will throw an IllegalStateException.
     *
     * @throws IllegalStateException if the Connector is not in a state that allows it to be deleted
     */
    void verifyCanDelete();

    void verifyCanStart();

    Connector getConnector();

    /**
     * @return the fully qualified class name of the underlying Connector implementation
     */
    String getComponentType();

    BundleCoordinate getBundleCoordinate();

    List<AllowableValue> fetchAllowableValues(String stepName, String groupName, String propertyName);

    List<AllowableValue> fetchAllowableValues(String stepName, String groupName, String propertyName, String filter);

    void initializeConnector(FrameworkConnectorInitializationContext initializationContext);

    void loadInitialFlow() throws FlowUpdateException;

    /**
     * <p>
     * Pause triggering asynchronous validation to occur when the connector is updated. Often times, it is necessary
     * to update several aspects of a connector, such as the properties and annotation data, at once. When this occurs,
     * we don't want to trigger validation for each update, so we can follow the pattern:
     * </p>
     *
     * <pre>
     * <code>
     * connectorNode.pauseValidationTrigger();
     * try {
     *   connectorNode.setProperties(properties);
     *   connectorNode.setAnnotationData(annotationData);
     * } finally {
     *   connectorNode.resumeValidationTrigger();
     * }
     * </code>
     * </pre>
     *
     * <p>
     * When calling this method, it is imperative that {@link #resumeValidationTrigger()} is always called within a {@code finally} block to
     * ensure that validation occurs.
     * </p>
     */
    void pauseValidationTrigger();

    /**
     * Resume triggering asynchronous validation to occur when the connector is updated. This method is to be used in conjunction
     * with {@link #pauseValidationTrigger()} as illustrated in its documentation. When this method is called, if the connector's Validation Status
     * is {@link ValidationStatus#VALIDATING}, connector validation will immediately be triggered asynchronously.
     */
    void resumeValidationTrigger();

    List<ConfigVerificationResult> verifyConfigurationStep(String configurationStepName, List<PropertyGroupConfiguration> propertyGroupConfigurations);

    List<ConfigVerificationResult> verify();

    ComponentLog getComponentLog();

    List<ConfigurationStep> getConfigurationSteps();

    FrameworkFlowContext getActiveFlowContext();

    FrameworkFlowContext getWorkingFlowContext();

    void discardWorkingConfiguration();

    // -------------------
    // The following methods should always be called via the ConnectorRepository in order to maintain proper
    // lifecycle management of the Connector.

    /**
     * Sets the name of the Connector. This method should only be invoked via the ConnectorRepository.
     * @param name the Connector's name
     */
    void setName(String name);

    /**
     * Performs validation logic that is defined by the Connector.
     * @return the ValidationState indicating the results of the validation
     */
    ValidationState performValidation();

    /**
     * Enables the Connector. This method should only be invoked via the ConnectorRepository.
     */
    void enable();

    /**
     * Disables the Connector. This method should only be invoked via the ConnectorRepository.
     */
    void disable();

    /**
     * Returns an Optional Duration indicating how long the Connector has been idle (i.e., not processed any FlowFiles and with no FlowFiles queued).
     * If there is any FlowFile queued, the Optional will be empty. Otherwise, the duration will indicate how long it has been since the
     * last FlowFile was acted upon.
     *
     * @return an Optional Duration indicating how long the Connector has been idle
     */
    Optional<Duration> getIdleDuration();

    /**
     * Returns the FlowFileTransferCounts that represents that amount of data sent and received
     * by this Connector since it was started.
     * @return the FlowFileTransferCounts for this Connector
     */
    FlowFileTransferCounts getFlowFileTransferCounts();

    /**
     * Starts the Connector. This method should only be invoked via the ConnectorRepository.
     * @param scheduler the ScheduledExecutorService to use for scheduling any tasks that the Connector needs to perform
     * @return a Future that will be completed when the Connector has started
     */
    Future<Void> start(FlowEngine scheduler);

    /**
     * Stops the Connector. This method should only be invoked via the ConnectorRepository.
     * @param scheduler the ScheduledExecutorService to use for scheduling any tasks that the Connector needs to perform
     * @return a Future that will be completed when the Connector has stopped
     */
    Future<Void> stop(FlowEngine scheduler);

    /**
     * Updates the configuration of one of the configuration steps. This method should only be invoked via the ConnectorRepository.
     * @param configurationStepName the name of the configuration step being set
     *                              (must match one of the names returned by {@link Connector#getConfigurationSteps(FlowContext)})
     *                              when providing the working flow context
     * @param propertyGroupConfigurations the list of PropertyGroupConfigurations for the given configuration step
     * @throws FlowUpdateException if unable to apply the configuration changes
     */
    void setConfiguration(String configurationStepName, List<PropertyGroupConfiguration> propertyGroupConfigurations) throws FlowUpdateException;

    void prepareForUpdate() throws FlowUpdateException;

    /**
     * Aborts the update preparation process. This method should only be invoked via the ConnectorRepository.
     * @param cause the reason for aborting the update preparation
     */
    // TODO: Should this return a Future<Void>?
    void abortUpdate(Throwable cause);

    /**
     * Notifies the Connector that the update process is finished and it can apply any changes that were made during the
     * update process. This method should only be invoked via the ConnectorRepository.
     * @throws FlowUpdateException if unable to apply the changes made during the update process
     */
    void applyUpdate() throws FlowUpdateException;

    /**
     * Inherits the given flow configuration into this Connector's active flow configuration.
     * @param flowConfiguration the flow configuration to inherit
     * @param flowContextBundle the bundle associated with the provided configuration
     * @throws FlowUpdateException if unable to inherit the given flow configuration
     * @throws IllegalStateException if the Connector is not in a state of UPDATING
     */
    void inheritConfiguration(List<VersionedConfigurationStep> flowConfiguration, Bundle flowContextBundle) throws FlowUpdateException;
}
