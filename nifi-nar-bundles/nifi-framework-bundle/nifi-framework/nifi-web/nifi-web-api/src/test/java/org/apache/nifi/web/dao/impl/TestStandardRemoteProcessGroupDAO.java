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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.exception.ValidationException;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.remote.RemoteGroupPort;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestStandardRemoteProcessGroupDAO {

    private void validate(final StandardRemoteProcessGroupDAO dao, final RemoteProcessGroupPortDTO dto, final String ... errMessageKeywords) {
        try {
            dao.verifyUpdateInputPort(dto.getGroupId(), dto);
            if (errMessageKeywords.length > 0) {
                fail("Validation should fail with keywords: " + Arrays.asList(errMessageKeywords));
            }
        } catch (ValidationException e) {
            if (errMessageKeywords.length == 0) {
                fail("Validation should pass, but failed with: " + e);
            }
            final List<String> validationErrors = e.getValidationErrors();
            assertEquals("Validation should return one validationErrors", 1, validationErrors.size());
            final String validationError = validationErrors.get(0);
            for (String errMessageKeyword : errMessageKeywords) {
                assertTrue("validation error message should contain " + errMessageKeyword + ", but was: " + validationError,
                        validationError.contains(errMessageKeyword));
            }
        }
    }

    @Test
    public void testVerifyUpdateInputPort() {
        final StandardRemoteProcessGroupDAO dao = new StandardRemoteProcessGroupDAO();

        final String remoteProcessGroupId = "remote-process-group-id";
        final String remoteProcessGroupInputPortId = "remote-process-group-input-port-id";

        final FlowController flowController = mock(FlowController.class);
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        final RemoteProcessGroup remoteProcessGroup = mock(RemoteProcessGroup.class);
        final RemoteGroupPort remoteGroupPort = mock(RemoteGroupPort.class);

        dao.setFlowController(flowController);
        when(flowController.getGroup(any())).thenReturn(processGroup);
        when(processGroup.findRemoteProcessGroup(eq(remoteProcessGroupId))).thenReturn(remoteProcessGroup);
        when(remoteProcessGroup.getInputPort(remoteProcessGroupInputPortId)).thenReturn(remoteGroupPort);
        when(remoteGroupPort.getName()).thenReturn("remote-group-port");

        final RemoteProcessGroupPortDTO dto = new RemoteProcessGroupPortDTO();
        dto.setGroupId(remoteProcessGroupId);
        dto.setId(remoteProcessGroupInputPortId);

        // Empty input values should pass validation.
        dao.verifyUpdateInputPort(remoteProcessGroupId, dto);

        // Concurrent tasks
        dto.setConcurrentlySchedulableTaskCount(0);
        validate(dao, dto, "Concurrent tasks", "positive integer");

        dto.setConcurrentlySchedulableTaskCount(2);
        validate(dao, dto);

        // Batch count
        dto.setBatchCount(-1);
        validate(dao, dto, "Batch count", "positive integer");

        dto.setBatchCount(0);
        validate(dao, dto);

        dto.setBatchCount(1000);
        validate(dao, dto);

        // Batch size
        dto.setBatchSize("AB");
        validate(dao, dto, "Batch size", "Data Size");

        dto.setBatchSize("10 days");
        validate(dao, dto, "Batch size", "Data Size");

        dto.setBatchSize("300MB");
        validate(dao, dto);

        // Batch duration
        dto.setBatchDuration("AB");
        validate(dao, dto, "Batch duration", "Time Unit");

        dto.setBatchDuration("10 KB");
        validate(dao, dto, "Batch duration", "Time Unit");

        dto.setBatchDuration("10 secs");
        validate(dao, dto);

    }

}