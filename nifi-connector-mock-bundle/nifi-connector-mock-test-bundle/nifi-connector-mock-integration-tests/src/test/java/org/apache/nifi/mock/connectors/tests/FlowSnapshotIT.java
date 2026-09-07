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

package org.apache.nifi.mock.connectors.tests;

import org.apache.nifi.components.connector.AssetReference;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.flow.VersionedAsset;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.mock.connector.StandardConnectorTestRunner;
import org.apache.nifi.mock.connector.server.ConnectorTestRunner;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class FlowSnapshotIT {
    private static final String CONNECTOR_CLASS = "org.apache.nifi.mock.connectors.FlowSnapshotConnector";
    private static final String CONFIGURATION_STEP = "Snapshot Configuration";
    private static final String ASSET_IDENTIFIER_PROPERTY = "Asset Identifier";
    private static final String ASSET_NAME = "certificate.pem";
    private static final String SENSITIVE_PARAMETER_NAME = "sensitive_parameter";
    private static final String ASSET_PARAMETER_NAME = "asset_parameter";

    @Test
    public void testWorkingAndActiveFlowSnapshotsPreserveParameterMetadata() throws IOException, FlowUpdateException {
        try (final ConnectorTestRunner runner = new StandardConnectorTestRunner.Builder()
                .connectorClassName(CONNECTOR_CLASS)
                .narLibraryDirectory(new File("target/libDir"))
                .build()) {

            final ByteArrayInputStream assetContents = new ByteArrayInputStream("certificate contents".getBytes(StandardCharsets.UTF_8));
            final AssetReference assetReference = runner.addAsset(ASSET_NAME, assetContents);
            final String assetIdentifier = assetReference.getAssetIdentifiers().iterator().next();

            runner.configure(CONFIGURATION_STEP, Map.of(ASSET_IDENTIFIER_PROPERTY, assetIdentifier));
            assertSnapshotParameters(runner.getWorkingFlowSnapshot(), assetIdentifier);

            runner.applyUpdate();
            assertSnapshotParameters(runner.getActiveFlowSnapshot(), assetIdentifier);
        }
    }

    private void assertSnapshotParameters(final VersionedExternalFlow snapshot, final String assetIdentifier) {
        assertEquals(1, snapshot.getParameterContexts().size());
        final VersionedParameterContext parameterContext = snapshot.getParameterContexts().values().iterator().next();

        final VersionedParameter sensitiveParameter = getParameter(parameterContext, SENSITIVE_PARAMETER_NAME);
        final VersionedParameter assetParameter = getParameter(parameterContext, ASSET_PARAMETER_NAME);
        assertTrue(sensitiveParameter.isSensitive());
        assertFalse(sensitiveParameter.isProvided());
        assertNull(sensitiveParameter.getValue());
        assertFalse(assetParameter.isSensitive());
        assertFalse(assetParameter.isProvided());
        assertNull(assetParameter.getValue());
        assertNotNull(assetParameter.getReferencedAssets());
        assertEquals(1, assetParameter.getReferencedAssets().size());

        final VersionedAsset versionedAsset = assetParameter.getReferencedAssets().getFirst();
        assertEquals(assetIdentifier, versionedAsset.getIdentifier());
        assertEquals(ASSET_NAME, versionedAsset.getName());
    }

    private VersionedParameter getParameter(final VersionedParameterContext parameterContext, final String name) {
        return parameterContext.getParameters().stream()
            .filter(parameter -> name.equals(parameter.getName()))
            .findFirst()
            .orElseThrow();
    }
}
