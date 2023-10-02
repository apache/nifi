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

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.ControllerServicesClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PropertyMigrationIT extends NiFiSystemIT {

    @AfterEach
    public void restoreNars() {
        switchNarsBack();
    }

    @Test
    public void testPropertyMigration() throws NiFiClientException, IOException {
        final ProcessorEntity migrate = getClientUtil().createProcessor("MigrateProperties");

        final Map<String, String> properties = migrate.getComponent().getConfig().getProperties();
        final Map<String, String> expectedProperties = new HashMap<>();
        expectedProperties.put("ingest-data", "true");
        expectedProperties.put("attr-to-add", null);
        expectedProperties.put("attr-value", null);
        expectedProperties.put("ignored", null);
        assertEquals(expectedProperties, properties);

        getClientUtil().updateProcessorProperties(migrate, Collections.singletonMap("attr-to-add", "greeting"));
        getClientUtil().updateProcessorProperties(migrate, Collections.singletonMap("attr-value", "Hi"));
        getClientUtil().updateProcessorProperties(migrate, Collections.singletonMap("ignored", "This is ignored"));

        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity connection = getClientUtil().createConnection(migrate, terminate, "success");
        getClientUtil().setAutoTerminatedRelationships(migrate, "failure");
        getClientUtil().setRetriedRelationships(migrate, "success");

        // Create Controller Services. Make the first depend on the second
        final ControllerServiceEntity firstService = getClientUtil().createControllerService("MigrationService");
        final ControllerServiceEntity secondService = getClientUtil().createControllerService("MigrationService");
        getClientUtil().updateControllerServiceProperties(firstService, Map.of("Start", "5", "Dependent Service", secondService.getId()));

        // Ensure that the reference count is correct
        final ControllerServicesClient servicesClient = getNifiClient().getControllerServicesClient();
        assertEquals(1, servicesClient.getControllerService(secondService.getId()).getComponent().getReferencingComponents().size());

        // Create a Reporting Task and set its properties
        final ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("MigrationReportingTask");
        getClientUtil().updateReportingTaskProperties(reportingTask, Map.of("Start", "15", "Invalid Property", "Invalid"));

        // Stop NiFi, switch out the system-tests-extensions nar for the alternate-config-nar, and restart
        getNiFiInstance().stop();
        switchOutNars();

        getNiFiInstance().start(true);

        // Ensure that the Processor's config was properly updated
        final ProcessorEntity updated = getNifiClient().getProcessorClient().getProcessor(migrate.getId());
        final Map<String, String> updatedProperties = updated.getComponent().getConfig().getProperties();

        final Map<String, String> expectedUpdatedProperties = Map.of("Ingest Data", "true",
                "Attribute to add", "greeting",
                "Attribute Value", "Hi",
                "New Property", "true");
        assertEquals(expectedUpdatedProperties, updatedProperties);

        final ProcessorConfigDTO updatedConfig = updated.getComponent().getConfig();
        assertEquals(Set.of("even", "odd"), updatedConfig.getRetriedRelationships());
        assertEquals(Set.of("broken"), updatedConfig.getAutoTerminatedRelationships());

        // Ensure that outgoing connections were properly updated
        final ConnectionEntity updatedConnection = getNifiClient().getConnectionClient().getConnection(connection.getId());
        final Set<String> selectedRelationships = updatedConnection.getComponent().getSelectedRelationships();
        assertEquals(Set.of("even", "odd"), selectedRelationships);

        // Ensure that the first Controller Service was properly updated and that its reference to the second service is properly handled.
        // I.e., the second service should no longer have any references.
        final ControllerServiceEntity updatedFirstService = servicesClient.getControllerService(firstService.getId());
        final Map<String, String> expectedServicesProperties = Map.of("Initial Value", "5");
        assertEquals(expectedServicesProperties, updatedFirstService.getComponent().getProperties());

        final ControllerServiceEntity updatedSecondService = servicesClient.getControllerService(secondService.getId());
        assertTrue(updatedSecondService.getComponent().getReferencingComponents().isEmpty());

        // Ensure that the Reporting Task was properly updated
        final ReportingTaskEntity updatedReportingTask = getNifiClient().getReportingTasksClient().getReportingTask(reportingTask.getId());
        final Map<String, String> expectedReportingTaskProperties = Map.of("Initial Value", "15", "Invalid Property", "Invalid");
        assertEquals(expectedReportingTaskProperties, updatedReportingTask.getComponent().getProperties());
    }

    private void switchOutNars() throws IOException {
        final File instanceDir = getNiFiInstance().getInstanceDirectory();
        final File lib = new File(instanceDir, "lib");
        final File alternateConfig = new File(lib, "alternate-config");

        // Move the nifi-system-test-extensions-nar out of the lib directory
        final File libNar = findFile(lib, "nifi-system-test-extensions-nar-.*");
        assertNotNull(libNar);
        final File libNarTarget = new File(alternateConfig, libNar.getName());
        assertTrue(libNar.renameTo(libNarTarget));

        final File alternateNar = findFile(alternateConfig, "nifi-alternate-config.*");
        assertNotNull(alternateNar);
        final File alternateNarTarget = new File(lib, alternateNar.getName());
        assertTrue(alternateNar.renameTo(alternateNarTarget));

        final File workDir = new File(instanceDir, "work/nar/extensions");
        deleteRecursively(workDir);
    }

    private void deleteRecursively(final File file) throws IOException {
        Files.walkFileTree(file.toPath(), new FileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(final Path file, final IOException exc) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });

    }

    private void switchNarsBack() {
        final File instanceDir = getNiFiInstance().getInstanceDirectory();
        final File lib = new File(instanceDir, "lib");
        final File alternateConfig = new File(lib, "alternate-config");

        // Move the nifi-system-test-extensions-nar out of the lib directory
        final File libNar = findFile(alternateConfig, "nifi-system-test-extensions-nar-.*");
        if (libNar != null) {
            final File libNarTarget = new File(lib, libNar.getName());
            assertTrue(libNar.renameTo(libNarTarget));
        }

        final File alternateNar = findFile(lib, "nifi-alternate-config.*");
        if (alternateNar != null) {
            final File alternateNarTarget = new File(alternateConfig, alternateNar.getName());
            assertTrue(alternateNar.renameTo(alternateNarTarget));
        }
    }

    private File findFile(final File dir, final String regex) {
        final Pattern pattern = Pattern.compile(regex);
        final File[] files = dir.listFiles(file -> pattern.matcher(file.getName()).find());
        if (files == null || files.length != 1) {
            return null;
        }
        return files[0];
    }
}
