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

package org.apache.nifi.tests.system.reportingtask;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.PropertyDescriptorEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReportingTaskIT extends NiFiSystemIT {

    private static final String SENSITIVE_PROPERTY_NAME = "Credentials";

    private static final String SENSITIVE_PROPERTY_VALUE = "Token";

    private static final Set<String> SENSITIVE_DYNAMIC_PROPERTY_NAMES = Collections.singleton(SENSITIVE_PROPERTY_NAME);


    @Test
    public void testReportingTaskDependingOnControllerService() throws NiFiClientException, IOException, InterruptedException {
        // Create a Count controller service and configure a property on the service.
        final ControllerServiceEntity controllerService = getClientUtil().createRootLevelControllerService("StandardCountService");
        final Map<String, String> controllerServiceProperties = new HashMap<>();
        controllerServiceProperties.put("Start Value", "1000000");
        getClientUtil().updateControllerServiceProperties(controllerService, controllerServiceProperties);
        getClientUtil().enableControllerService(controllerService);

        // Create a Reporting Task that references the Controller Service
        ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("WriteToFileReportingTask");
        final Map<String, String> reportingTaskProperties = new HashMap<>();
        reportingTaskProperties.put("Count Service", controllerService.getId());
        final File reportingTaskFile = new File("target/reporting-task-count.txt");
        Files.deleteIfExists(reportingTaskFile.toPath());
        reportingTaskProperties.put("Filename", reportingTaskFile.getAbsolutePath());
        reportingTask = getClientUtil().updateReportingTaskProperties(reportingTask, reportingTaskProperties);

        // Schedule reporting task to run every 10 milliseconds.
        reportingTask.getComponent().setSchedulingPeriod("10 millis");
        getNifiClient().getReportingTasksClient().updateReportingTask(reportingTask);
        getClientUtil().waitForReportingTaskValid(reportingTask.getId());

        // Start reporting Task
        getClientUtil().startReportingTask(reportingTask);

        // Wait for the reporting task to run at least 5 times.
        waitFor(() -> getCount(reportingTaskFile) >= 1_000_005);

        // Stop the instance, delete the file, and restart.
        getNiFiInstance().stop();
        waitFor(() -> reportingTaskFile.delete() || !reportingTaskFile.exists());
        getNiFiInstance().start(true);

        // Upon restart, we should start counting again at 1,000,000.
        // Wait until the reporting task runs enough times to create the file again and write a value of at least 1,000,005.
        waitFor(() -> getCount(reportingTaskFile) >= 1_000_005);
    }

    @Test
    public void testGetPropertyDescriptor() throws NiFiClientException, IOException {
        final ReportingTaskEntity reportingTaskEntity = getClientUtil().createReportingTask("SensitiveDynamicPropertiesReportingTask");

        final PropertyDescriptorEntity propertyDescriptorEntity = getNifiClient().getReportingTasksClient().getPropertyDescriptor(reportingTaskEntity.getId(), SENSITIVE_PROPERTY_NAME, null);
        final PropertyDescriptorDTO propertyDescriptor = propertyDescriptorEntity.getPropertyDescriptor();
        assertFalse(propertyDescriptor.isSensitive());
        assertTrue(propertyDescriptor.isDynamic());

        final PropertyDescriptorEntity sensitivePropertyDescriptorEntity = getNifiClient().getReportingTasksClient().getPropertyDescriptor(reportingTaskEntity.getId(), SENSITIVE_PROPERTY_NAME, true);
        final PropertyDescriptorDTO sensitivePropertyDescriptor = sensitivePropertyDescriptorEntity.getPropertyDescriptor();
        assertTrue(sensitivePropertyDescriptor.isSensitive());
        assertTrue(sensitivePropertyDescriptor.isDynamic());
    }

    @Test
    public void testSensitiveDynamicPropertiesNotSupported() throws NiFiClientException, IOException {
        final ReportingTaskEntity reportingTaskEntity = getClientUtil().createReportingTask("WriteToFileReportingTask");
        final ReportingTaskDTO component = reportingTaskEntity.getComponent();
        assertFalse(component.getSupportsSensitiveDynamicProperties());

        component.setSensitiveDynamicPropertyNames(SENSITIVE_DYNAMIC_PROPERTY_NAMES);

        getNifiClient().getReportingTasksClient().updateReportingTask(reportingTaskEntity);

        getClientUtil().waitForReportingTaskValidationStatus(reportingTaskEntity.getId(), ReportingTaskDTO.INVALID);
    }

    @Test
    public void testSensitiveDynamicPropertiesSupportedConfigured() throws NiFiClientException, IOException {
        final ReportingTaskEntity reportingTaskEntity = getClientUtil().createReportingTask("SensitiveDynamicPropertiesReportingTask");
        final ReportingTaskDTO component = reportingTaskEntity.getComponent();
        assertTrue(component.getSupportsSensitiveDynamicProperties());

        component.setSensitiveDynamicPropertyNames(SENSITIVE_DYNAMIC_PROPERTY_NAMES);
        component.setProperties(Collections.singletonMap(SENSITIVE_PROPERTY_NAME, SENSITIVE_PROPERTY_VALUE));

        getNifiClient().getReportingTasksClient().updateReportingTask(reportingTaskEntity);

        final ReportingTaskEntity updatedReportingTaskEntity = getNifiClient().getReportingTasksClient().getReportingTask(reportingTaskEntity.getId());
        final ReportingTaskDTO updatedComponent = updatedReportingTaskEntity.getComponent();

        final Map<String, String> properties = updatedComponent.getProperties();
        assertNotSame(SENSITIVE_PROPERTY_VALUE, properties.get(SENSITIVE_PROPERTY_NAME));

        final Map<String, PropertyDescriptorDTO> descriptors = updatedComponent.getDescriptors();
        final PropertyDescriptorDTO descriptor = descriptors.get(SENSITIVE_PROPERTY_NAME);
        assertNotNull(descriptor);
        assertTrue(descriptor.isSensitive());
        assertTrue(descriptor.isDynamic());

        getClientUtil().waitForReportingTaskValid(reportingTaskEntity.getId());
    }

    private long getCount(final File file) throws IOException {
        final byte[] bytes = Files.readAllBytes(file.toPath());
        final String contents = new String(bytes, StandardCharsets.UTF_8);
        return Long.parseLong(contents);
    }
}
