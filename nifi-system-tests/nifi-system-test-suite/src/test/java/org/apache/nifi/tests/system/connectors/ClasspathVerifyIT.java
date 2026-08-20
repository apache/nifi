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

package org.apache.nifi.tests.system.connectors;

import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.dynamicclasspath.DynamicallyLoadedType;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * System tests that verify additional classpath resources are loaded during connector configuration verification,
 * Connector Method discovery and invocation use the Working component classpath after Parameter updates and after
 * Processor or Controller Service property updates, and consecutive invocations preserve component instance state.
 */
public class ClasspathVerifyIT extends NiFiSystemIT {

    private static final String STEP_NAME = "Classpath Configuration";
    private static final String METHOD_SIGNATURE_STEP_NAME = "Method Signature Configuration";
    private static final String STRATEGY_PROCESSOR_VERIFY = "Processor Verify";
    private static final String STRATEGY_CONNECTOR_METHOD = "Connector Method";
    private static final String STRATEGY_CONNECTOR_METHOD_STATE = "Connector Method State";
    private static final String LOAD_CLASS_STEP = "Load Class From Classpath";
    private static final String CONNECTOR_METHOD_STEP = "Invoke loadClass Connector Method";
    private static final String CONNECTOR_METHOD_STATE_STEP = "Preserve Connector Method Component State";
    private static final String METHOD_SIGNATURE_STEP = "Discover Connector Method With Dynamic Signature";

    private static final String CLASSPATH_RESOURCE = "Classpath Resource";
    private static final String CLASS_TO_LOAD = "Class to Load";
    private static final String VERIFICATION_STRATEGY = "Verification Strategy";
    private static final String CLASSPATH_APPLICATION = "Classpath Application";
    private static final String APPLICATION_PROCESSOR_PROPERTY = "Processor Property";
    private static final String APPLICATION_CONTROLLER_SERVICE_PROPERTY = "Controller Service Property";

    private static final String DYNAMIC_CLASSPATH_CLASS = DynamicallyLoadedType.class.getName();
    private static final String UNKNOWN_CLASS = "org.apache.nifi.tests.system.DoesNotExistOnClasspath";

    @Test
    public void testProcessorVerifyUsesTemporaryClasspathWhenUncommitted() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ClasspathVerifyConnector");
        final String assetId = uploadDynamicClasspathAsset(connector.getId());

        final List<ConfigVerificationResultDTO> results = getClientUtil().verifyConnectorStepConfigWithReferences(
                connector.getId(), STEP_NAME, createVerifyReferences(assetId, DYNAMIC_CLASSPATH_CLASS, STRATEGY_PROCESSOR_VERIFY));

        assertSuccessfulClasspathStep(results, LOAD_CLASS_STEP);
    }

    @Test
    public void testConnectorMethodLoadsClassFromCommittedClasspath() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ClasspathVerifyConnector");
        final String assetId = uploadDynamicClasspathAsset(connector.getId());

        getClientUtil().configureConnectorWithReferences(connector.getId(), STEP_NAME, createVerifyReferences(assetId, DYNAMIC_CLASSPATH_CLASS, STRATEGY_CONNECTOR_METHOD));
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connector.getId());

        final List<ConfigVerificationResultDTO> results = getClientUtil().verifyConnectorStepConfigWithReferences(
                connector.getId(), STEP_NAME, createVerifyReferences(assetId, DYNAMIC_CLASSPATH_CLASS, STRATEGY_CONNECTOR_METHOD));

        assertSuccessfulClasspathStep(results, CONNECTOR_METHOD_STEP);
    }

    @Test
    public void testConnectorMethodPreservesWorkingComponentState() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ClasspathVerifyConnector");
        final String assetId = uploadDynamicClasspathAsset(connector.getId());

        getClientUtil().configureConnectorWithReferences(connector.getId(), STEP_NAME, createVerifyReferences(assetId, DYNAMIC_CLASSPATH_CLASS, STRATEGY_CONNECTOR_METHOD_STATE));

        final List<ConfigVerificationResultDTO> results = getClientUtil().verifyConnectorStepConfigWithReferences(
                connector.getId(), STEP_NAME, createVerifyReferences(assetId, DYNAMIC_CLASSPATH_CLASS, STRATEGY_CONNECTOR_METHOD_STATE));

        assertSuccessfulClasspathStep(results, CONNECTOR_METHOD_STATE_STEP);
    }

    @Test
    public void testConnectorMethodLoadsAssetWithMethodSignatureDependency() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("MethodSignatureVerifyConnector");
        final String assetId = uploadDynamicClasspathAsset(connector.getId());

        getClientUtil().configureConnectorWithReferences(connector.getId(), METHOD_SIGNATURE_STEP_NAME, createAssetReferences(assetId));

        final List<ConfigVerificationResultDTO> results = getClientUtil().verifyConnectorStepConfigWithReferences(connector.getId(), METHOD_SIGNATURE_STEP_NAME, createAssetReferences(assetId));

        assertSuccessfulClasspathStep(results, METHOD_SIGNATURE_STEP);
    }

    @Test
    public void testConnectorMethodLoadsAssetWhenSetAsProcessorProperty() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("MethodSignatureVerifyConnector");
        final String assetId = uploadDynamicClasspathAsset(connector.getId());

        getClientUtil().configureConnectorWithReferences(connector.getId(), METHOD_SIGNATURE_STEP_NAME, createMethodSignatureReferences(assetId, APPLICATION_PROCESSOR_PROPERTY));

        final List<ConfigVerificationResultDTO> results = getClientUtil().verifyConnectorStepConfigWithReferences(connector.getId(), METHOD_SIGNATURE_STEP_NAME, createMethodSignatureReferences(assetId, APPLICATION_PROCESSOR_PROPERTY));

        assertSuccessfulClasspathStep(results, METHOD_SIGNATURE_STEP);
    }

    @Test
    public void testConnectorMethodLoadsAssetWhenSetAsControllerServiceProperty() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("MethodSignatureVerifyConnector");
        final String assetId = uploadDynamicClasspathAsset(connector.getId());

        getClientUtil().configureConnectorWithReferences(connector.getId(), METHOD_SIGNATURE_STEP_NAME, createMethodSignatureReferences(assetId, APPLICATION_CONTROLLER_SERVICE_PROPERTY));

        final List<ConfigVerificationResultDTO> results = getClientUtil().verifyConnectorStepConfigWithReferences(connector.getId(), METHOD_SIGNATURE_STEP_NAME, createMethodSignatureReferences(assetId, APPLICATION_CONTROLLER_SERVICE_PROPERTY));

        assertSuccessfulClasspathStep(results, METHOD_SIGNATURE_STEP);
    }

    @Test
    public void testConnectorMethodFailsWhenClassMissingFromClasspath() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ClasspathVerifyConnector");
        final String assetId = uploadDynamicClasspathAsset(connector.getId());

        getClientUtil().configureConnectorWithReferences(connector.getId(), STEP_NAME, createVerifyReferences(assetId, UNKNOWN_CLASS, STRATEGY_CONNECTOR_METHOD));
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connector.getId());

        final List<ConfigVerificationResultDTO> results = getClientUtil().verifyConnectorStepConfigWithReferences(
                connector.getId(), STEP_NAME, createVerifyReferences(assetId, UNKNOWN_CLASS, STRATEGY_CONNECTOR_METHOD));

        assertFailedClasspathStep(results, CONNECTOR_METHOD_STEP);
    }

    @Test
    public void testProcessorVerifyUsesLiveClasspathWhenCommitted() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ClasspathVerifyConnector");
        final String assetId = uploadDynamicClasspathAsset(connector.getId());

        getClientUtil().configureConnectorWithReferences(connector.getId(), STEP_NAME, createVerifyReferences(assetId, DYNAMIC_CLASSPATH_CLASS, STRATEGY_PROCESSOR_VERIFY));
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connector.getId());

        final List<ConfigVerificationResultDTO> results = getClientUtil().verifyConnectorStepConfigWithReferences(
                connector.getId(), STEP_NAME, createVerifyReferences(assetId, DYNAMIC_CLASSPATH_CLASS, STRATEGY_PROCESSOR_VERIFY));

        assertSuccessfulClasspathStep(results, LOAD_CLASS_STEP);
    }

    @Test
    public void testProcessorVerifyFailsForUnknownClass() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("ClasspathVerifyConnector");
        final String assetId = uploadDynamicClasspathAsset(connector.getId());

        getClientUtil().configureConnectorWithReferences(connector.getId(), STEP_NAME, createVerifyReferences(assetId, DYNAMIC_CLASSPATH_CLASS, STRATEGY_PROCESSOR_VERIFY));
        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connector.getId());

        final List<ConfigVerificationResultDTO> results = getClientUtil().verifyConnectorStepConfigWithReferences(
                connector.getId(), STEP_NAME, createVerifyReferences(assetId, UNKNOWN_CLASS, STRATEGY_PROCESSOR_VERIFY));

        assertFailedClasspathStep(results, LOAD_CLASS_STEP);
    }

    private String uploadDynamicClasspathAsset(final String connectorId) throws IOException, NiFiClientException {
        // DynamicallyLoadedType is available on the test JVM classpath through a provided-scope dependency, which
        // resolves to the module's built jar without adding the type to the NiFi server runtime assembly. Uploading that
        // jar as the Connector asset makes the type available to the running component only through its dynamic classpath.
        final File dynamicClasspathJar = locateDynamicClasspathJar();

        final File assetCopy = new File("target/dynamic-classpath-" + connectorId + ".jar");
        Files.copy(dynamicClasspathJar.toPath(), assetCopy.toPath(), StandardCopyOption.REPLACE_EXISTING);

        final AssetEntity assetEntity = getNifiClient().getConnectorClient().createAsset(connectorId, assetCopy.getName(), assetCopy);
        assertNotNull(assetEntity);
        assertNotNull(assetEntity.getAsset());
        return assetEntity.getAsset().getId();
    }

    private File locateDynamicClasspathJar() {
        final URL location = DynamicallyLoadedType.class.getProtectionDomain().getCodeSource().getLocation();
        final File jarFile;
        try {
            jarFile = new File(location.toURI());
        } catch (final URISyntaxException e) {
            throw new IllegalStateException("Could not resolve the dynamic classpath jar from location " + location, e);
        }

        if (!jarFile.isFile() || !jarFile.getName().endsWith(".jar")) {
            throw new IllegalStateException("Expected the nifi-system-test-dynamic-classpath dependency to resolve to a jar file but found "
                    + jarFile.getAbsolutePath() + "; ensure the module is installed so the provided-scope dependency resolves to a jar");
        }

        return jarFile;
    }

    private Map<String, ConnectorValueReferenceDTO> createVerifyReferences(final String assetId, final String classToLoad, final String strategy) {
        final Map<String, ConnectorValueReferenceDTO> propertyValues = new HashMap<>();
        propertyValues.put(CLASSPATH_RESOURCE, createAssetReference(assetId));
        propertyValues.put(CLASS_TO_LOAD, createStringLiteralReference(classToLoad));
        propertyValues.put(VERIFICATION_STRATEGY, createStringLiteralReference(strategy));
        return propertyValues;
    }

    private Map<String, ConnectorValueReferenceDTO> createAssetReferences(final String assetId) {
        final Map<String, ConnectorValueReferenceDTO> propertyValues = new HashMap<>();
        propertyValues.put(CLASSPATH_RESOURCE, createAssetReference(assetId));
        return propertyValues;
    }

    private Map<String, ConnectorValueReferenceDTO> createMethodSignatureReferences(final String assetId, final String classpathApplication) {
        final Map<String, ConnectorValueReferenceDTO> propertyValues = createAssetReferences(assetId);
        propertyValues.put(CLASSPATH_APPLICATION, createStringLiteralReference(classpathApplication));
        return propertyValues;
    }

    private ConnectorValueReferenceDTO createAssetReference(final String assetId) {
        final ConnectorValueReferenceDTO assetReference = new ConnectorValueReferenceDTO();
        assetReference.setValueType("ASSET_REFERENCE");
        assetReference.setAssetReferences(List.of(new AssetReferenceDTO(assetId)));
        return assetReference;
    }

    private ConnectorValueReferenceDTO createStringLiteralReference(final String value) {
        final ConnectorValueReferenceDTO valueReference = new ConnectorValueReferenceDTO();
        valueReference.setValueType("STRING_LITERAL");
        valueReference.setValue(value);
        return valueReference;
    }

    private void assertSuccessfulClasspathStep(final List<ConfigVerificationResultDTO> results, final String stepName) {
        assertNotNull(results);
        final ConfigVerificationResultDTO classpathResult = findStep(results, stepName);
        assertEquals(Outcome.SUCCESSFUL.name(), classpathResult.getOutcome(), classpathResult.getExplanation());
    }

    private void assertFailedClasspathStep(final List<ConfigVerificationResultDTO> results, final String stepName) {
        assertNotNull(results);
        final ConfigVerificationResultDTO classpathResult = findStep(results, stepName);
        assertEquals(Outcome.FAILED.name(), classpathResult.getOutcome(), classpathResult.getExplanation());
    }

    private ConfigVerificationResultDTO findStep(final List<ConfigVerificationResultDTO> results, final String stepName) {
        return results.stream()
                .filter(result -> stepName.equals(result.getVerificationStepName()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Expected verification step '" + stepName + "' in results: " + results.stream()
                        .map(ConfigVerificationResultDTO::getVerificationStepName)
                        .toList()));
    }
}
