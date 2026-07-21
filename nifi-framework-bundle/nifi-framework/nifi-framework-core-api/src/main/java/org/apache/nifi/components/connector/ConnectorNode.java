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
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.VersionedComponent;
import org.apache.nifi.components.connector.migration.ConnectorMigrationContext;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.connectable.FlowFileTransferCounts;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedConfigurationStep;
import org.apache.nifi.logging.ComponentLog;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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

    String getCanonicalClassName();

    BundleCoordinate getBundleCoordinate();

    /**
     * @return an immutable map of the Connector's logging attributes: the framework-managed identity keys
     *         (such as connector identifier, name, component type, and bundle coordinate) merged with any
     *         provider-supplied custom attributes. These are the attributes applied to the MDC of the Connector's
     *         managed flow and surfaced on Connector metrics. Never {@code null}. The default implementation
     *         returns an empty map.
     */
    default Map<String, String> getLoggingAttributes() {
        return Map.of();
    }

    /**
     * Replaces the provider-supplied custom logging attributes for this Connector, merging them with the
     * framework-managed identity keys (see {@link #getLoggingAttributes()}). This is invoked by the framework
     * when the Connector node is added or restored, using attributes sourced from the
     * {@link ConnectorConfigurationProvider} (if one is configured). Keys reserved by the framework are ignored.
     * The default implementation is a no-op.
     *
     * @param attributes the proposed custom attributes; {@code null} or empty clears any previously supplied
     *                   custom attributes (the framework-managed keys remain)
     */
    default void setCustomLoggingAttributes(final Map<String, String> attributes) {
    }

    /**
     * Returns whether or not the underlying extension is missing (i.e., the Connector is a GhostConnector).
     * @return true if the extension is missing, false otherwise
     */
    boolean isExtensionMissing();

    List<DescribedValue> fetchAllowableValues(String stepName, String propertyName);

    List<DescribedValue> fetchAllowableValues(String stepName, String propertyName, String filter);

    void initializeConnector(FrameworkConnectorInitializationContext initializationContext);

    void loadInitialFlow() throws FlowUpdateException;

    boolean isMigrationSupported(ConnectorMigrationContext context);

    /**
     * Reports whether the Connector has been modified since it was created. The result is used to gate the
     * {@code MIGRATE} allowable action and to verify migration eligibility at migrate time so that an incoming
     * Versioned flow cannot silently overwrite user modifications.
     *
     * <p>A Connector derives its managed flow entirely from its configuration, so this is determined from durable
     * state that is reproduced on every NiFi restart: the Connector's Active and Working configurations and the
     * component state accumulated by its managed flow. A Connector is considered modified when any configured
     * property differs from the property's declared default value, or when any Processor or Controller Service in
     * the managed flow has stored component state. Simply starting and stopping the Connector without configuring
     * it or accumulating state does not make it modified.
     *
     * @return {@code true} when the Connector's configuration has diverged from its defaults or its managed flow has
     *         accumulated component state; {@code false} when the Connector is still in the state it was created in
     */
    boolean isModified();

    /**
     * Discards the working flow context, if any, and rebuilds it from the active flow context's configuration.
     * The framework calls this on the success path of a Connector migration so that any subsequent configure-step
     * interactions begin from the migrated flow rather than from the working state that existed before migration.
     */
    void recreateWorkingFlowContext();

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

    /**
     * Indicates whether validation triggering is currently paused.
     * @return true if validation triggering is paused, false otherwise
     */
    boolean isValidationPaused();

    List<ConfigVerificationResult> verifyConfigurationStep(String configurationStepName, StepConfiguration configurationOverrides);

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
     * Triggers validation of all processors and controller services within the Connector's managed ProcessGroup.
     *
     * @param validationTrigger the ValidationTrigger to use for triggering component validation
     */
    void validateComponents(ValidationTrigger validationTrigger);

    /**
     * Returns the current validation status of the connector.
     * @return the current ValidationStatus
     */
    ValidationStatus getValidationStatus();

    /**
     * Returns the validation errors for the connector, or an empty collection if the connector is valid.
     * @return the validation errors, or an empty collection if valid
     */
    Collection<ValidationResult> getValidationErrors();


    ValidationState getValidationState();

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
     * Allows the Connector to drain any in-flight data while not accepting any new data.
     */
    Future<Void> drainFlowFiles();

    /**
     * Cancels the draining of FlowFiles that is currently in progress.
     * @throws IllegalStateException if the Connector is not currently draining FlowFiles
     */
    void cancelDrainFlowFiles();

    /**
     * Verifies that the Connector can cancel draining FlowFiles.
     * @throws IllegalStateException if not in a state where draining FlowFiles can be cancelled
     */
    void verifyCancelDrainFlowFiles() throws IllegalStateException;

    /**
     * Verifies that the Connector can have its FlowFiles purged.
     * @throws IllegalStateException if not in a state where FlowFiles can be purged
     */
    void verifyCanPurgeFlowFiles() throws IllegalStateException;

    /**
     * Purges all FlowFiles from the Connector, immediately dropping the data.
     *
     * @param requestor the user requesting the purge. This will be recorded in the associated provenance DROP events.
     * @return a Future that will be completed when the purge operation is finished
     */
    Future<Void> purgeFlowFiles(String requestor);

    /**
     * Updates the configuration of one of the configuration steps. This method should only be invoked via the ConnectorRepository.
     * @param configurationStepName the name of the configuration step being set
     *                              (must match one of the names returned by {@link Connector#getConfigurationSteps()})
     * @param configuration the configuration for the given configuration step
     * @throws FlowUpdateException if unable to apply the configuration changes
     */
    void setConfiguration(String configurationStepName, StepConfiguration configuration) throws FlowUpdateException;

    /**
     * Replaces the configuration of the named step on the working flow context with the given configuration.
     * Unlike {@link #setConfiguration(String, StepConfiguration)}, which merges the incoming properties with
     * any existing properties for the step, this method treats the supplied configuration as the authoritative
     * full state for the step: any property not present in {@code configuration} is removed from the step.
     *
     * <p>If applying the configuration changes either the raw or the resolved property values, the Connector
     * is notified via {@link Connector#onConfigurationStepConfigured} so the embedded flow and Parameter
     * Context can be brought up to date. If nothing changed, no notification is performed.</p>
     *
     * <p>Intended for use by the framework when reconciling the working flow context against an external
     * {@link ConnectorConfigurationProvider}, whose view is treated as authoritative. This method should
     * only be invoked via the ConnectorRepository.</p>
     *
     * @param configurationStepName the name of the configuration step being replaced
     *                              (must match one of the names returned by {@link Connector#getConfigurationSteps()})
     * @param configuration the full configuration for the given configuration step
     * @throws FlowUpdateException if unable to apply the configuration changes
     */
    void replaceWorkingConfiguration(String configurationStepName, StepConfiguration configuration) throws FlowUpdateException;

    void transitionStateForUpdating();

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
     * @param activeFlowConfiguration the active flow configuration to inherit
     * @param workingFlowConfiguration the working flow configuration to inherit
     * @param flowContextBundle the bundle associated with the provided configuration
     * @throws FlowUpdateException if unable to inherit the given flow configuration
     * @throws IllegalStateException if the Connector is not in a state of UPDATING
     */
    void inheritConfiguration(List<VersionedConfigurationStep> activeFlowConfiguration, List<VersionedConfigurationStep> workingFlowConfiguration,
        Bundle flowContextBundle) throws FlowUpdateException;

    /**
     * Rebuilds this Connector's managed Process Group from the merged configuration a {@code MigratableConnector}
     * produced during {@code migrateConfiguration(...)}, and drives
     * {@link Connector#applyUpdate(org.apache.nifi.components.connector.components.FlowContext,
     *   org.apache.nifi.components.connector.components.FlowContext) Connector.applyUpdate(workingContext, activeContext)}
     * so the managed Process Group is rebuilt from it. Uses the same {@code applyUpdate(...)} path the framework uses
     * on restart, so a single Connector code path rebuilds the managed flow in both cases.
     *
     * <p>
     * The supplied {@code mergedConfiguration} is the working configuration the connector mutated through the
     * migration context: the framework seeded it with a clone of this Connector's active configuration before
     * {@code migrateConfiguration(...)} ran, and the connector's {@code setProperties(...)} and
     * {@code replaceProperties(...)} calls were applied to it directly. The active configuration is intentionally not
     * mutated here so that a failure in the state-migration phase that follows can be rolled back by simply restoring
     * the initial flow; the merged configuration is returned and the framework writes it onto the active configuration
     * via {@link #commitMigratedConfiguration} only after the state-migration phase has succeeded.
     * </p>
     *
     * <p>
     * This method is only invoked by the framework's migration manager. It does not require the Connector to be in
     * any particular state-transition state and does not transition the Connector into {@code UPDATING}/{@code UPDATED}
     * the way {@link #applyUpdate()} does; migration starts and ends with the Connector {@code STOPPED}.
     * </p>
     *
     * @param mergedConfiguration the merged working configuration produced by the connector during migration
     * @return the merged configuration that the framework must later commit onto the active configuration via
     *         {@link #commitMigratedConfiguration}
     * @throws FlowUpdateException when the merged configuration cannot be applied
     */
    ConnectorConfiguration applyMigratedConfiguration(MutableConnectorConfigurationContext mergedConfiguration) throws FlowUpdateException;

    /**
     * Commits the merged configuration returned by {@link #applyMigratedConfiguration} onto this Connector's active
     * configuration so the migration outcome is persisted to {@code flow.json.gz}. Each step in {@code mergedConfiguration}
     * is written via {@code replaceProperties} so the resulting active configuration matches the merged configuration
     * exactly, including property removals.
     *
     * <p>
     * The framework calls this method only after the state-migration phase and all staged component-state writes have
     * succeeded. Until this commit runs, the active configuration still holds the pre-migration values, so a failure
     * in either migration phase can be rolled back by restoring the initial flow without having to revert active
     * configuration mutations.
     * </p>
     *
     * @param mergedConfiguration the configuration produced by {@link #applyMigratedConfiguration} during this migration
     */
    void commitMigratedConfiguration(ConnectorConfiguration mergedConfiguration);

    /**
     * Marks the connector as invalid with the given subject and explanation. This is used when a flow update
     * fails during initialization or flow synchronization to indicate the connector cannot operate.
     *
     * @param subject the subject of the validation failure
     * @param explanation the reason the connector is invalid
     */
    void markInvalid(final String subject, final String explanation);

    /**
     * Returns the list of available actions that can be performed on this Connector.
     * Each action includes whether it is currently allowed and, if not, the reason why.
     * @return the list of available actions
     */
    List<ConnectorAction> getAvailableActions();

    /**
     * Verifies that the Connector is in a state that allows it to be transitioned into TROUBLESHOOTING.
     * @throws IllegalStateException if the Connector cannot enter Troubleshooting mode
     */
    void verifyCanEnterTroubleshooting();

    /**
     * Verifies that the Connector is in a state that allows it to be transitioned out of TROUBLESHOOTING.
     * The Connector must be in TROUBLESHOOTING. Additionally, all components within the Connector's Managed Process Group
     * must be in a stopped / disabled state.
     * @throws IllegalStateException if the Connector cannot exit Troubleshooting mode
     */
    void verifyCanEndTroubleshooting();

    /**
     * Transitions the Connector into TROUBLESHOOTING state. This method should only be invoked via the ConnectorRepository.
     * @throws IllegalStateException if the Connector cannot enter Troubleshooting mode
     */
    void enterTroubleshooting();

    /**
     * Restores the Connector's state to TROUBLESHOOTING without stopping any components within the Managed Process Group
     * and without running the pre-conditions enforced by {@link #enterTroubleshooting()}. This is intended to be used
     * only by the flow synchronization layer when restoring a Connector that was persisted while in Troubleshooting
     * mode so that components inside the Managed Process Group retain their persisted runtime state (for example,
     * processors that were running when NiFi shut down stay running after restart).
     */
    void restoreTroubleshootingState();

    /**
     * Transitions the Connector out of TROUBLESHOOTING state. The Connector's Managed Process Group will be restored
     * to the Connector's authoritative view of the flow as reported by {@link Connector#getActiveFlow}. This method should
     * only be invoked via the ConnectorRepository.
     *
     * @throws IllegalStateException if the Connector cannot exit Troubleshooting mode
     * @throws FlowUpdateException if unable to apply the authoritative flow (for example because data is queued in a
     * Connection that would be removed by the restore)
     */
    void endTroubleshooting() throws FlowUpdateException;
}
