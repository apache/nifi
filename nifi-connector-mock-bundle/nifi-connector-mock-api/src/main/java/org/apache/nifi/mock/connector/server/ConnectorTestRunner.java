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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.components.connector.StepConfiguration;

import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public interface ConnectorTestRunner extends Closeable {
    String SECRET_PROVIDER_ID = "TestRunnerSecretsManager";
    String SECRET_PROVIDER_NAME = "TestRunnerSecretsManager";

    void applyUpdate() throws FlowUpdateException;

    void configure(String stepName, StepConfiguration configuration) throws FlowUpdateException;

    void configure(String stepName, Map<String, String> propertyValues) throws FlowUpdateException;

    void configure(String stepName, Map<String, String> propertyValues, Map<String, ConnectorValueReference> propertyReferences) throws FlowUpdateException;

    SecretReference createSecretReference(String secretName);

    ConnectorConfigVerificationResult verifyConfiguration(String stepName, Map<String, String> propertyValueOverrides);

    ConnectorConfigVerificationResult verifyConfiguration(String stepName, Map<String, String> propertyValueOverrides, Map<String, ConnectorValueReference> referenceOverrides);

    ConnectorConfigVerificationResult verifyConfiguration(String stepName, StepConfiguration configurationOverrides);

    void addSecret(String name, String value);

    void startConnector();

    void stopConnector();

    void waitForDataIngested(Duration maxWaitTime);

    void waitForIdle(Duration maxWaitTime);

    void waitForIdle(Duration minimumIdleTime, Duration maxWaitTime);

    List<ValidationResult> validate();
}
