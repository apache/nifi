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
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class ReportingTaskIT extends NiFiSystemIT {

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

    private long getCount(final File file) throws IOException {
        final byte[] bytes = Files.readAllBytes(file.toPath());
        final String contents = new String(bytes, StandardCharsets.UTF_8);
        return Long.parseLong(contents);
    }
}
