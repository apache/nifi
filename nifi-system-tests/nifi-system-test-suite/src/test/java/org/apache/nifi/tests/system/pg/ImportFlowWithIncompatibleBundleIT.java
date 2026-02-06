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
package org.apache.nifi.tests.system.pg;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ParameterProviderDTO;
import org.apache.nifi.web.api.entity.ParameterProviderEntity;
import org.apache.nifi.web.api.entity.ParameterProvidersEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * System tests to verify that importing/uploading a flow with components that have incompatible bundle versions
 * properly falls back to using available bundle versions instead of creating Ghost components.
 */
public class ImportFlowWithIncompatibleBundleIT extends NiFiSystemIT {

    private static final String PROPERTIES_PARAMETER_PROVIDER_TYPE = TEST_PARAM_PROVIDERS_PACKAGE + ".PropertiesParameterProvider";
    private static final String INCOMPATIBLE_VERSION = "0.0.0-DOES-NOT-EXIST";

    @TempDir
    private Path tempDir;

    /**
     * Tests that when uploading a flow with a ParameterContext that references a ParameterProvider with an
     * incompatible bundle version, NiFi falls back to using the only available bundle version instead of
     * creating a Ghost component.
     *
     * This test verifies the fix for a bug where the bundle version fallback logic was not applied to
     * ParameterProviders during flow upload, causing them to be ghosted even when only one compatible
     * version was available.
     *
     * This test specifically uses the upload endpoint (POST /process-groups/{id}/process-groups/upload)
     * which exercises the code path in ProcessGroupResource that requires the fix.
     */
    @Test
    public void testUploadFlowWithParameterProviderIncompatibleBundleVersion() throws NiFiClientException, IOException, InterruptedException {
        final RegisteredFlowSnapshot snapshot = createSnapshotWithParameterProviderIncompatibleBundle();

        final File tempFile = tempDir.resolve("flow-snapshot.json").toFile();
        final ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.writeValue(tempFile, snapshot);

        final ProcessGroupEntity uploadedGroup = getNifiClient().getProcessGroupClient().upload(
                "root",
                tempFile,
                "Uploaded Test Group",
                100.0,
                100.0
        );

        assertNotNull(uploadedGroup, "Uploaded process group should not be null");
        assertNotNull(uploadedGroup.getId(), "Uploaded process group should have an ID");

        final ParameterProvidersEntity parameterProviders = getNifiClient().getFlowClient().getParamProviders();
        assertNotNull(parameterProviders);
        assertNotNull(parameterProviders.getParameterProviders());
        assertFalse(parameterProviders.getParameterProviders().isEmpty(),
                "Expected at least one parameter provider to be created");

        final ParameterProviderEntity createdProvider = parameterProviders.getParameterProviders().stream()
                .filter(pp -> pp.getComponent().getType().equals(PROPERTIES_PARAMETER_PROVIDER_TYPE))
                .findFirst()
                .orElse(null);

        assertNotNull(createdProvider, "Expected PropertiesParameterProvider to be created");

        final ParameterProviderDTO providerDto = createdProvider.getComponent();
        assertNotNull(providerDto);
        assertNotNull(providerDto.getBundle(), "Bundle should not be null");
        assertFalse(INCOMPATIBLE_VERSION.equals(providerDto.getBundle().getVersion()),
                "Bundle version should NOT be the incompatible version - should have fallen back to available version");
        assertEquals(getNiFiVersion(), providerDto.getBundle().getVersion(),
                "Bundle version should be the NiFi framework version (fallback)");
        assertFalse(providerDto.getType().startsWith("(Missing)"),
                "Parameter provider should not be a Ghost component - type should not start with '(Missing)'");
        assertEquals(PROPERTIES_PARAMETER_PROVIDER_TYPE, providerDto.getType(),
                "Parameter provider type should match");
    }

    /**
     * Creates a RegisteredFlowSnapshot that contains a ParameterContext referencing a ParameterProvider
     * with a bundle version that does not exist in the system.
     */
    private RegisteredFlowSnapshot createSnapshotWithParameterProviderIncompatibleBundle() {
        final String parameterProviderId = UUID.randomUUID().toString();

        final Bundle incompatibleBundle = new Bundle();
        incompatibleBundle.setGroup(NIFI_GROUP_ID);
        incompatibleBundle.setArtifact(TEST_EXTENSIONS_ARTIFACT_ID);
        incompatibleBundle.setVersion(INCOMPATIBLE_VERSION);

        final ParameterProviderReference providerReference = new ParameterProviderReference();
        providerReference.setIdentifier(parameterProviderId);
        providerReference.setName("Test Parameter Provider");
        providerReference.setType(PROPERTIES_PARAMETER_PROVIDER_TYPE);
        providerReference.setBundle(incompatibleBundle);

        final Map<String, ParameterProviderReference> parameterProviders = new HashMap<>();
        parameterProviders.put(parameterProviderId, providerReference);

        final VersionedParameterContext versionedParameterContext = new VersionedParameterContext();
        versionedParameterContext.setIdentifier(UUID.randomUUID().toString());
        versionedParameterContext.setName("Test Parameter Context");
        versionedParameterContext.setDescription("Parameter context for testing bundle fallback");
        versionedParameterContext.setParameterProvider(parameterProviderId);
        versionedParameterContext.setParameterGroupName("Parameters");
        versionedParameterContext.setSynchronized(true);

        final VersionedParameter versionedParameter = new VersionedParameter();
        versionedParameter.setName("test-param");
        versionedParameter.setValue("test-value");
        versionedParameter.setSensitive(false);
        versionedParameter.setProvided(true);

        final Set<VersionedParameter> parameters = new HashSet<>();
        parameters.add(versionedParameter);
        versionedParameterContext.setParameters(parameters);

        final Map<String, VersionedParameterContext> parameterContexts = new HashMap<>();
        parameterContexts.put(versionedParameterContext.getName(), versionedParameterContext);

        final VersionedProcessGroup versionedProcessGroup = new VersionedProcessGroup();
        versionedProcessGroup.setIdentifier(UUID.randomUUID().toString());
        versionedProcessGroup.setName("Test Process Group");
        versionedProcessGroup.setParameterContextName("Test Parameter Context");

        final RegisteredFlowSnapshot snapshot = new RegisteredFlowSnapshot();
        snapshot.setFlowContents(versionedProcessGroup);
        snapshot.setParameterContexts(parameterContexts);
        snapshot.setParameterProviders(parameterProviders);

        return snapshot;
    }
}
