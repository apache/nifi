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
import org.apache.nifi.toolkit.client.ControllerServicesClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PropertyMigrationIT extends NiFiSystemIT {
    private static final String SERVICE = "Service";

    @AfterEach
    public void restoreNars() {
        // Stop the NiFi instance, ensure that the nifi-system-test-extensions-nar and nifi-alternate-config-extensions bundles
        // are where they need to be. Then, restart the instance so that everything is in the right state for the next test that
        // will run
        getNiFiInstance().stop();
        switchNarsBack();
        getNiFiInstance().start(true);
    }

    @Test
    public void testControllerServiceCreated() throws NiFiClientException, IOException {
        final ProcessGroupEntity group1 = getClientUtil().createProcessGroup("Group 1", "root");
        final ProcessGroupEntity group2 = getClientUtil().createProcessGroup("Group 2", "root");

        final ProcessorEntity proc1 = getClientUtil().createProcessor("MigrateProperties", group1.getId());
        final ProcessorEntity proc2 = getClientUtil().createProcessor("MigrateProperties", group1.getId());
        final ProcessorEntity proc3 = getClientUtil().createProcessor("MigrateProperties", group1.getId());
        final ProcessorEntity proc4 = getClientUtil().createProcessor("MigrateProperties", group2.getId());

        // Update proc1 and proc2 with the same values.
        // Set same values for proc4, which is in a different group.
        final Map<String, String> proc1Properties = Map.of(
                "attr-to-add", "greeting",
                "attr-value", "Hi",
                "ignored", "17"
        );
        getClientUtil().updateProcessorProperties(proc1, proc1Properties);
        getClientUtil().updateProcessorProperties(proc2, proc1Properties);
        getClientUtil().updateProcessorProperties(proc4, proc1Properties);

        final Map<String, String> proc3Properties = new HashMap<>(proc1Properties);
        proc3Properties.put("ignored", "41");
        getClientUtil().updateProcessorProperties(proc3, proc3Properties);

        // Stop NiFi, switch out the system-tests-extensions nar for the alternate-config-nar, and restart
        getNiFiInstance().stop();
        switchOutNars();
        getNiFiInstance().start(true);

        // Procs 1 and 2 should have the same value for the Controller Service.
        // Procs 3 and 4 should each have different values
        final ControllerServicesClient serviceClient = getNifiClient().getControllerServicesClient();

        final Map<String, String> proc1UpdatedProps = getProperties(proc1);
        final Map<String, String> proc2UpdatedProps = getProperties(proc2);
        final Map<String, String> proc3UpdatedProps = getProperties(proc3);
        final Map<String, String> proc4UpdatedProps = getProperties(proc4);

        final Set<String> serviceIds = new HashSet<>();
        for (final Map<String, String> propertiesMap : List.of(proc1UpdatedProps, proc2UpdatedProps, proc3UpdatedProps, proc4UpdatedProps)) {
            final String serviceId = propertiesMap.get(SERVICE);
            assertNotNull(serviceId);
            serviceIds.add(serviceId);

            assertEquals("Deprecated Value", propertiesMap.get("Deprecated"));
            assertFalse(propertiesMap.containsKey("Deprecated Found"));
        }

        // Should be 3 different services
        assertEquals(3, serviceIds.size());

        // Procs 1 and 2 should reference the same service.
        assertEquals(proc1UpdatedProps.get(SERVICE), proc2UpdatedProps.get(SERVICE));

        // Services for procs 1-3 should be in group 1
        for (final String serviceId : List.of(proc1UpdatedProps.get(SERVICE), proc2UpdatedProps.get(SERVICE), proc3UpdatedProps.get(SERVICE))) {
            assertEquals(group1.getId(), serviceClient.getControllerService(serviceId).getParentGroupId());
        }

        // Service for proc 4 should be in group 2
        assertEquals(group2.getId(), serviceClient.getControllerService(proc4UpdatedProps.get(SERVICE)).getParentGroupId());

        // Ensure that the service's properties were also migrated, since the processor mapped the "ignored" value to the old property name of the service.
        final ControllerServiceEntity service1 = serviceClient.getControllerService(proc1UpdatedProps.get(SERVICE));
        final Map<String, String> service1Props = service1.getComponent().getProperties();
        assertEquals(Map.of("Initial Value", "17"), service1Props);
        assertEquals(2, service1.getComponent().getReferencingComponents().size());

        final ControllerServiceEntity service4 = serviceClient.getControllerService(proc4UpdatedProps.get(SERVICE));
        final Map<String, String> service4Props = service4.getComponent().getProperties();
        assertEquals(Map.of("Initial Value", "17"), service4Props);
        assertEquals(1, service4.getComponent().getReferencingComponents().size());

        final ControllerServiceEntity service3 = serviceClient.getControllerService(proc3UpdatedProps.get(SERVICE));
        final Map<String, String> service3Props = service3.getComponent().getProperties();
        assertEquals(Map.of("Initial Value", "41"), service3Props);
        assertEquals(1, service3.getComponent().getReferencingComponents().size());
    }

    private Map<String, String> getProperties(final ProcessorEntity processor) throws NiFiClientException, IOException {
        return getNifiClient().getProcessorClient().getProcessor(processor.getId()).getComponent().getConfig().getProperties();
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

        final Map<String, String> expectedUpdatedProperties = new HashMap<>();
        expectedUpdatedProperties.put("Ingest Data", "true");
        expectedUpdatedProperties.put("Attribute to add", "greeting");
        expectedUpdatedProperties.put("Attribute Value", "Hi");
        expectedUpdatedProperties.put("New Property", "true");
        expectedUpdatedProperties.put("Service", null);
        expectedUpdatedProperties.put("Deprecated", "Deprecated Value");
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
        moveNars(lib, "nifi-system-test-extensions-nar-.*", alternateConfig);

        // Move the nifi-system-test-extensions-services-nar out of the lib directory
        moveNars(lib, "nifi-system-test-extensions-services-nar-.*", alternateConfig);

        // Move the nifi-system-test-extensions-services-api-nar out of the lib directory
        moveNars(lib, "nifi-system-test-extensions-services-api-nar-.*", alternateConfig);

        moveNars(alternateConfig, "nifi-alternate-config.*", lib);

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

        // Move the nifi-system-test-extensions-nar back to the lib directory
        moveNars(alternateConfig, "nifi-system-test-extensions-nar-.*", lib);

        // Move the nifi-system-test-extensions-services-nar back to the lib directory
        moveNars(alternateConfig, "nifi-system-test-extensions-services-nar-.*", lib);

        // Move the nifi-system-test-extensions-services-api-nar back to the lib directory
        moveNars(alternateConfig, "nifi-system-test-extensions-services-api-nar-.*", lib);

        moveNars(lib, "nifi-alternate-config.*", alternateConfig);
    }

    private File findFile(final File dir, final String regex) {
        final Pattern pattern = Pattern.compile(regex);
        final File[] files = dir.listFiles(file -> pattern.matcher(file.getName()).find());
        if (files == null || files.length != 1) {
            return null;
        }
        return files[0];
    }

    private void moveNars(File source, String regex, File target) {
        final File libNar = findFile(source, regex);
        assertNotNull(libNar);
        final File libNarTarget = new File(target, libNar.getName());
        assertTrue(libNar.renameTo(libNarTarget));
    }
}
