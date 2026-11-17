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

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.ConnectorClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System test that validates Connectors can use Parameter Contexts with inheritance,
 * sensitive parameters (via SECRET_REFERENCE), and asset-referencing parameters.
 */
public class ConnectorParameterContextIT extends NiFiSystemIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectorParameterContextIT.class);
    private static final String SENSITIVE_SECRET_VALUE = "my-super-secret-value";
    private static final String ASSET_FILE_CONTENT = "Hello, World!";

    @Test
    public void testParameterContextInheritanceWithSensitiveAndAssetParameters() throws NiFiClientException, IOException, InterruptedException {
        final File sensitiveOutputFile = new File("target/sensitive.txt");
        final File assetOutputFile = new File("target/asset.txt");

        sensitiveOutputFile.delete();
        assetOutputFile.delete();

        final ParameterProviderEntity paramProvider = getClientUtil().createParameterProvider("PropertiesParameterProvider");
        getClientUtil().updateParameterProviderProperties(paramProvider, Map.of("parameters", "secret=" + SENSITIVE_SECRET_VALUE));

        final ConnectorEntity connector = getClientUtil().createConnector("ParameterContextConnector");
        assertNotNull(connector);
        assertNotNull(connector.getId());

        final String connectorId = connector.getId();
        final ConnectorClient connectorClient = getNifiClient().getConnectorClient();

        final File assetFile = new File("src/test/resources/sample-assets/helloworld.txt");
        final AssetEntity assetEntity = connectorClient.createAsset(connectorId, assetFile.getName(), assetFile);
        assertNotNull(assetEntity);
        assertNotNull(assetEntity.getAsset());
        assertNotNull(assetEntity.getAsset().getId());

        final String uploadedAssetId = assetEntity.getAsset().getId();

        final ConnectorValueReferenceDTO secretRef = getClientUtil().createSecretValueReference(
                paramProvider.getId(), "secret", "PropertiesParameterProvider.Parameters.secret");

        final ConnectorValueReferenceDTO assetRef = new ConnectorValueReferenceDTO();
        assetRef.setValueType("ASSET_REFERENCE");
        assetRef.setAssetReferences(List.of(new AssetReferenceDTO(uploadedAssetId)));

        final Map<String, ConnectorValueReferenceDTO> propertyValues = new HashMap<>();
        propertyValues.put("Sensitive Value", secretRef);
        propertyValues.put("Asset File", assetRef);
        propertyValues.put("Sensitive Output File", createStringLiteralRef(sensitiveOutputFile.getAbsolutePath()));
        propertyValues.put("Asset Output File", createStringLiteralRef(assetOutputFile.getAbsolutePath()));

        getClientUtil().configureConnectorWithReferences(connectorId, "Parameter Context Configuration", propertyValues);
        LOGGER.info("Applying configuration to Connector...");
        getClientUtil().applyConnectorUpdate(connector);
        LOGGER.info("Waiting for Connector to become valid...");
        getClientUtil().waitForValidConnector(connectorId);

        LOGGER.info("Connector is valid; starting Connector...");
        getClientUtil().startConnector(connectorId);

        LOGGER.info("Waiting for output files to be created...");
        waitFor(() -> sensitiveOutputFile.exists() && assetOutputFile.exists());

        assertTrue(sensitiveOutputFile.exists(), "Sensitive output file should exist");
        assertTrue(assetOutputFile.exists(), "Asset output file should exist");

        final String sensitiveContent = Files.readString(sensitiveOutputFile.toPath()).trim();
        final String assetContent = Files.readString(assetOutputFile.toPath()).trim();

        assertEquals(SENSITIVE_SECRET_VALUE, sensitiveContent, "Sensitive output file should contain the secret value");
        assertEquals(ASSET_FILE_CONTENT, assetContent, "Asset output file should contain the asset file contents");
    }

    private ConnectorValueReferenceDTO createStringLiteralRef(final String value) {
        final ConnectorValueReferenceDTO ref = new ConnectorValueReferenceDTO();
        ref.setValueType("STRING_LITERAL");
        ref.setValue(value);
        return ref;
    }
}
