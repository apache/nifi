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

package org.apache.nifi.tests.system.verification;

import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.apache.nifi.web.api.entity.ReportingTaskRunStatusEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VerifiableReportingTaskSystemIT extends NiFiSystemIT {

    @Test
    public void testVerificationWithValidConfigWhenComponentValid() throws NiFiClientException, IOException, InterruptedException {
        final ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("EnsureReportingTaskConfigurationCorrect");

        final Map<String, String> properties = Collections.singletonMap("Successful Verification", "true");
        getClientUtil().updateReportingTask(reportingTask, properties);

        final List<ConfigVerificationResultDTO> resultList = getClientUtil().verifyReportingTaskConfig(reportingTask.getId(), properties);
        assertEquals(3, resultList.size());

        // First verification result will be component validation.
        assertEquals(Outcome.SUCCESSFUL.name(), resultList.get(0).getOutcome());
        // Second verification result will be verification results
        assertEquals(Outcome.SUCCESSFUL.name(), resultList.get(1).getOutcome());
        // Third verification result is for Fail On Primary Node
        assertEquals(Outcome.SKIPPED.name(), resultList.get(2).getOutcome());
    }


    @Test
    public void testVerifyWithInvalidConfigWhenComponentValid() throws NiFiClientException, IOException, InterruptedException {
        final ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("EnsureReportingTaskConfigurationCorrect");

        final Map<String, String> properties = Collections.singletonMap("Successful Verification", "true");
        getClientUtil().updateReportingTask(reportingTask, properties);

        // Verify with properties that will give us failed verification
        final Map<String, String> invalidProperties = Collections.singletonMap("Successful Verification", "false");
        final List<ConfigVerificationResultDTO> resultList = getClientUtil().verifyReportingTaskConfig(reportingTask.getId(), invalidProperties);
        assertEquals(3, resultList.size());

        // First verification result will be component validation.
        assertEquals(Outcome.SUCCESSFUL.name(), resultList.get(0).getOutcome());
        // Second verification result will be FAILED because the 'Successful Verification' property is set to false
        assertEquals(Outcome.FAILED.name(), resultList.get(1).getOutcome());
        // Third verification result is for Fail On Primary Node
        assertEquals(Outcome.SKIPPED.name(), resultList.get(2).getOutcome());
    }

    @Test
    public void testVerificationWithValidConfigWhenComponentInvalid() throws NiFiClientException, IOException, InterruptedException {
        final ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("EnsureReportingTaskConfigurationCorrect");

        final Map<String, String> invalidProperties = Collections.singletonMap("Successful Verification", "foo");
        getClientUtil().updateReportingTask(reportingTask, invalidProperties);

        final Map<String, String> validProperties = Collections.singletonMap("Successful Verification", "true");
        final List<ConfigVerificationResultDTO> resultList = getClientUtil().verifyReportingTaskConfig(reportingTask.getId(), validProperties);
        assertEquals(3, resultList.size());

        assertEquals(Outcome.SUCCESSFUL.name(), resultList.get(0).getOutcome());
        assertEquals(Outcome.SUCCESSFUL.name(), resultList.get(1).getOutcome());
        assertEquals(Outcome.SKIPPED.name(), resultList.get(2).getOutcome());
    }

    @Test
    public void testVerifyWithInvalidConfigWhenComponentInvalid() throws InterruptedException, IOException, NiFiClientException {
        final ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("EnsureReportingTaskConfigurationCorrect");

        final Map<String, String> invalidProperties = Collections.singletonMap("Successful Verification", "foo");
        getClientUtil().updateReportingTask(reportingTask, invalidProperties);

        final Map<String, String> otherInvalidProperties = Collections.singletonMap("Successful Verification", "bar");
        final List<ConfigVerificationResultDTO> resultList = getClientUtil().verifyReportingTaskConfig(reportingTask.getId(), otherInvalidProperties);
        assertEquals(1, resultList.size());

        for (final ConfigVerificationResultDTO resultDto : resultList) {
            assertEquals(Outcome.FAILED.name(), resultDto.getOutcome());
        }
    }

    @Test
    public void testVerificationWithValidConfigWhenComponentRunning() throws IOException, NiFiClientException {
        final ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("EnsureReportingTaskConfigurationCorrect");

        final Map<String, String> properties = Collections.singletonMap("Successful Verification", "true");
        getClientUtil().updateReportingTask(reportingTask, properties);

        // Start reporting task
        final ReportingTaskRunStatusEntity runStatusEntity = new ReportingTaskRunStatusEntity();
        runStatusEntity.setRevision(reportingTask.getRevision());
        runStatusEntity.setState("RUNNING");
        getNifiClient().getReportingTasksClient().activateReportingTask(reportingTask.getId(), runStatusEntity);

        assertThrows(NiFiClientException.class, () -> getClientUtil().verifyReportingTaskConfig(reportingTask.getId(), properties));
    }


    @Test
    public void testVerifyWhenExceptionThrown() throws InterruptedException, IOException, NiFiClientException {
        final ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("EnsureReportingTaskConfigurationCorrect");

        final Map<String, String> properties = new HashMap<>();
        properties.put("Successful Verification", "true");
        properties.put("Exception on Verification", "true");
        getClientUtil().updateReportingTask(reportingTask, properties);

        final List<ConfigVerificationResultDTO> resultList = getClientUtil().verifyReportingTaskConfig(reportingTask.getId(), properties);
        assertEquals(2, resultList.size());

        // Results should show that validation is successful but that there was a failure in performing verification
        assertEquals(Outcome.SUCCESSFUL.name(), resultList.get(0).getOutcome());
        assertEquals(Outcome.FAILED.name(), resultList.get(1).getOutcome());
    }

    @Test
    public void testValidProcessorWithoutVerifiableReportingTaskAnnotation() throws NiFiClientException, IOException, InterruptedException {
        final ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("WriteToFileReportingTask");

        // Even though processor does not implement VerifiableProcessor, validation should still be run
        final Map<String, String> properties = new HashMap<>();
        properties.put("Filename", "./logs");
        properties.put("Text", "Hello World");

        final List<ConfigVerificationResultDTO> resultList = getClientUtil().verifyReportingTaskConfig(reportingTask.getId(), properties);
        assertEquals(1, resultList.size());

        // Even though GenerateFlowFile is not connected, it should be valid because connections are not considered when verifying the processor
        assertEquals(Outcome.SUCCESSFUL.name(), resultList.get(0).getOutcome());
    }

    @Test
    public void testInvalidConfigForProcessorWithoutVerifiableReportingTaskAnnotation() throws NiFiClientException, IOException, InterruptedException {
        final ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("WriteToFileReportingTask");

        final Map<String, String> properties = new HashMap<>();
        properties.put("Filename", "/foo-i-do-not-exist");
        properties.put("Text", "Hello World");

        final List<ConfigVerificationResultDTO> resultList = getClientUtil().verifyReportingTaskConfig(reportingTask.getId(), properties);
        assertEquals(1, resultList.size());

        //assertEquals("Unexpected results: " + resultList, Outcome.FAILED.name(), resultList.get(0).getOutcome()); // NIFI-9717
    }

}
