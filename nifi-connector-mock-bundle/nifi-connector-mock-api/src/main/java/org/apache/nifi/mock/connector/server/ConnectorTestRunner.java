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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.AssetReference;
import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.components.connector.StepConfiguration;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.io.Closeable;
import java.io.File;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public interface ConnectorTestRunner extends Closeable {

    /**
     * Applies the current working configuration to the Connector. If the Connector was running prior to the update,
     * it will be restarted after the update is applied. If the Connector was stopped, it will remain stopped.
     *
     * @throws FlowUpdateException if the update cannot be applied
     */
    void applyUpdate() throws FlowUpdateException;

    /**
     * Configures a specific step of the Connector using the given StepConfiguration.
     *
     * @param stepName the name of the configuration step to configure
     * @param configuration the configuration to apply to the step
     * @throws FlowUpdateException if the configuration cannot be applied
     */
    void configure(String stepName, StepConfiguration configuration) throws FlowUpdateException;

    /**
     * Configures a specific step of the Connector using the given property values as string literals.
     *
     * @param stepName the name of the configuration step to configure
     * @param propertyValues a map of property names to their string values
     * @throws FlowUpdateException if the configuration cannot be applied
     */
    void configure(String stepName, Map<String, String> propertyValues) throws FlowUpdateException;

    /**
     * Configures a specific step of the Connector using both string property values and typed value references
     * such as secret references or asset references.
     *
     * @param stepName the name of the configuration step to configure
     * @param propertyValues a map of property names to their string values
     * @param propertyReferences a map of property names to their ConnectorValueReference instances
     * @throws FlowUpdateException if the configuration cannot be applied
     */
    void configure(String stepName, Map<String, String> propertyValues, Map<String, ConnectorValueReference> propertyReferences) throws FlowUpdateException;

    /**
     * Creates a SecretReference that points to a secret managed by the test runner's built-in secrets manager.
     *
     * @param secretName the name of the secret to reference
     * @return a SecretReference for use in connector configuration
     */
    SecretReference createSecretReference(String secretName);

    /**
     * Verifies the configuration of the specified step, optionally overriding property values with the provided map.
     *
     * @param stepName the name of the configuration step to verify
     * @param propertyValueOverrides a map of property names to override values
     * @return the result of the configuration verification
     */
    ConnectorConfigVerificationResult verifyConfiguration(String stepName, Map<String, String> propertyValueOverrides);

    /**
     * Verifies the configuration of the specified step, optionally overriding property values and references.
     *
     * @param stepName the name of the configuration step to verify
     * @param propertyValueOverrides a map of property names to override values
     * @param referenceOverrides a map of property names to ConnectorValueReference overrides
     * @return the result of the configuration verification
     */
    ConnectorConfigVerificationResult verifyConfiguration(String stepName, Map<String, String> propertyValueOverrides, Map<String, ConnectorValueReference> referenceOverrides);

    /**
     * Verifies the configuration of the specified step using a StepConfiguration for overrides.
     *
     * @param stepName the name of the configuration step to verify
     * @param configurationOverrides the StepConfiguration containing property overrides
     * @return the result of the configuration verification
     */
    ConnectorConfigVerificationResult verifyConfiguration(String stepName, StepConfiguration configurationOverrides);

    /**
     * Registers a secret with the test runner's built-in secrets manager so that it can be referenced
     * by the Connector during configuration and execution.
     *
     * @param name the name of the secret
     * @param value the value of the secret
     */
    void addSecret(String name, String value);

    /**
     * Adds an asset to the Connector's asset manager from a local file.
     *
     * @param file the file to add as an asset
     * @return an AssetReference that can be used in connector configuration to reference the asset
     */
    AssetReference addAsset(File file);

    /**
     * Adds an asset to the Connector's asset manager from an InputStream.
     *
     * @param assetName the name to assign to the asset
     * @param contents the InputStream providing the asset content
     * @return an AssetReference that can be used in connector configuration to reference the asset
     */
    AssetReference addAsset(String assetName, InputStream contents);

    /**
     * Starts the Connector, beginning processing of data through its managed flow.
     */
    void startConnector();

    /**
     * Stops the Connector, halting all data processing in its managed flow.
     */
    void stopConnector();

    /**
     * Blocks until the Connector has received at least one FlowFile, or until the specified timeout elapses.
     *
     * @param maxWaitTime the maximum duration to wait for data ingestion
     * @throws RuntimeException if the timeout elapses before any data is ingested
     */
    void waitForDataIngested(Duration maxWaitTime);

    /**
     * Blocks until the Connector becomes idle (no active processing), or until the specified timeout elapses.
     *
     * @param maxWaitTime the maximum duration to wait for the Connector to become idle
     * @throws RuntimeException if the timeout elapses before the Connector becomes idle
     */
    void waitForIdle(Duration maxWaitTime);

    /**
     * Blocks until the Connector has been idle for at least the specified minimum duration,
     * or until the maximum wait time elapses.
     *
     * @param minimumIdleTime the minimum duration the Connector must be idle before returning
     * @param maxWaitTime the maximum duration to wait
     * @throws RuntimeException if the timeout elapses before the Connector has been idle long enough
     */
    void waitForIdle(Duration minimumIdleTime, Duration maxWaitTime);

    /**
     * Performs validation on the Connector and returns any validation errors.
     *
     * @return a list of ValidationResult objects representing validation failures, or an empty list if the Connector is valid
     */
    List<ValidationResult> validate();

    /**
     * Returns the HTTP port on which the embedded Jetty server is listening, or -1 if no server is running.
     *
     * @return the HTTP port, or -1 if not applicable
     */
    default int getHttpPort() {
        return -1;
    }

    /**
     * Fetches the allowable values for a property in the specified configuration step.
     *
     * @param stepName the name of the configuration step containing the property
     * @param propertyName the name of the property whose allowable values are to be fetched
     * @return the list of allowable values for the property
     */
    List<DescribedValue> fetchAllowableValues(String stepName, String propertyName);

    /**
     * Returns a {@link VersionedExternalFlow} representing the current state of the Active Flow Context.
     * The Active Flow Context is the flow that is currently running (or most recently ran) in the Connector.
     * This is useful for making assertions about how the flow is configured after updates have been applied.
     *
     * @return the VersionedExternalFlow for the Active Flow Context
     */
    VersionedExternalFlow getActiveFlowSnapshot();

    /**
     * Returns a {@link VersionedExternalFlow} representing the current state of the Working Flow Context.
     * The Working Flow Context is the flow that reflects configuration changes that have been made
     * but not yet applied. This is useful for making assertions about how the flow will be configured
     * once the update is applied.
     *
     * @return the VersionedExternalFlow for the Working Flow Context
     */
    VersionedExternalFlow getWorkingFlowSnapshot();
}
