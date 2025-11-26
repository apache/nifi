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
package org.apache.nifi.tests.system.migration;

import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedStandaloneNiFiInstanceFactory;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ControllerServicesEntity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MigratePropertiesCountServiceIT extends NiFiSystemIT {
    private static final String PARENT_GROUP_ID = "c1a920de-019a-1000-6510-f3bca7dd9c69";

    private static final int CONFIGURED_CONTROLLER_SERVICES = 12;

    /**
     * Override setup method with Timeout annotation to verify Controller Service validation timing
     *
     * @param testInfo Test Information
     * @throws IOException Thrown on setup failures
     */
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    @BeforeEach
    @Override
    public void setup(final TestInfo testInfo) throws IOException {
        super.setup(testInfo);
    }

    /**
     * Get Instance Factory using flow.json with property names that require renaming
     *
     * @return NiFi Instance Factory
     */
    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        final Path flowJsonInputPath = Paths.get("src/test/resources/flows/migrate-properties/flow.json");
        final Path flowJsonOutputPath = Paths.get("target/migrate-properties-flow.json.gz").toAbsolutePath();
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
        getClientUtil().waitForControllerServicesEnabled(PARENT_GROUP_ID);

        final ControllerServicesEntity servicesEntity = getNifiClient().getFlowClient().getControllerServices(PARENT_GROUP_ID);
        assertEquals(CONFIGURED_CONTROLLER_SERVICES, servicesEntity.getControllerServices().size());
    }
}
