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

import org.apache.nifi.stream.io.GZIPOutputStream;
import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedStandaloneNiFiInstanceFactory;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * System test that verifies that when a flow.json.gz contains a Connector whose
 * bundle version does not exist, but only one version of that Connector type is
 * available, the Connector is created with the correct/available version.
 */
class ConnectorVersionResolutionIT extends NiFiSystemIT {
    private static final String CONNECTOR_ID = "11111111-1111-1111-1111-111111111111";
    private static final String NONEXISTENT_VERSION = "1.0.0-nonexistent";

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        final Path flowJsonInputPath = Paths.get("src/test/resources/flows/connector-version-mismatch/flow.json");
        final Path flowJsonOutputPath = Paths.get("target/connector-version-mismatch-flow.json.gz").toAbsolutePath();

        try (final InputStream inputStream = Files.newInputStream(flowJsonInputPath);
             final OutputStream outputStream = new GZIPOutputStream(Files.newOutputStream(flowJsonOutputPath))) {
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
    void testConnectorCreatedWithCorrectVersionWhenOnlyOneVersionExists() throws NiFiClientException, IOException {
        final ConnectorEntity connector = getNifiClient().getConnectorClient().getConnector(CONNECTOR_ID);
        assertNotNull(connector);
        assertNotNull(connector.getComponent());

        final BundleDTO bundle = connector.getComponent().getBundle();
        assertNotNull(bundle, "Connector bundle should not be null");
        assertEquals("org.apache.nifi", bundle.getGroup());
        assertEquals("nifi-system-test-extensions-nar", bundle.getArtifact());

        assertNotEquals(NONEXISTENT_VERSION, bundle.getVersion(), "Connector should not have the nonexistent version from flow.json");
        assertEquals(getNiFiVersion(), bundle.getVersion(), "Connector should be created with the current NiFi version since only one version exists");
    }
}
