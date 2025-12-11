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
package org.apache.nifi.tests.system.controllerservice;

import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedStandaloneNiFiInstanceFactory;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.status.ControllerServiceStatusDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ControllerServiceEnablingFailureIT extends NiFiSystemIT {
    private static final String PARENT_GROUP_ID = "c1a920de-019a-1000-6510-f3bca7dd9c69";

    private static final String ENABLING_STATUS = "ENABLING";

    /**
     * Get Instance Factory using flow.json with property names that require renaming
     *
     * @return NiFi Instance Factory
     */
    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        final Path flowJsonInputPath = Paths.get("src/test/resources/flows/controller-service-enabling-failure/flow.json");
        final Path flowJsonOutputPath = Paths.get("target/controller-service-enabling-failure-flow.json.gz").toAbsolutePath();
        try (
                InputStream inputStream = Files.newInputStream(flowJsonInputPath);
                OutputStream outputStream = new GZIPOutputStream(Files.newOutputStream(flowJsonOutputPath))
        ) {
            inputStream.transferTo(outputStream);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to compress Flow Configuration [%s]".formatted(flowJsonInputPath), e);
        }

        return new SpawnedStandaloneNiFiInstanceFactory(
            new InstanceConfiguration.Builder()
                    .bootstrapConfig("src/test/resources/conf/default/bootstrap.conf")
                    .instanceDirectory("target/standalone-instance")
                    .flowJson(flowJsonOutputPath.toFile())
                    .overrideNifiProperties(getNifiPropertiesOverrides())
                    .build()
        );
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Test
    void testControllerServices() throws NiFiClientException, IOException {
        final ControllerServicesEntity servicesEntity = getNifiClient().getFlowClient().getControllerServices(PARENT_GROUP_ID);
        assertEquals(1, servicesEntity.getControllerServices().size());

        final ControllerServiceEntity controllerService = servicesEntity.getControllerServices().iterator().next();
        final ControllerServiceStatusDTO status = controllerService.getStatus();
        assertEquals(ValidationStatus.VALID.toString(), status.getValidationStatus());

        // ENABLING but not ENABLED based on LifecycleFailureService implementation and framework enabling attempts
        assertEquals(ENABLING_STATUS, status.getRunStatus());
    }
}
