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
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.ConfigVerificationResultDTO;
import org.apache.nifi.web.api.entity.ReportingTaskEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClusteredVerifiableReportingTaskSystemIT extends VerifiableReportingTaskSystemIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testDifferentResultsFromDifferentNodes() throws InterruptedException, IOException, NiFiClientException {
        final ReportingTaskEntity reportingTask = getClientUtil().createReportingTask("EnsureReportingTaskConfigurationCorrect");

        final Map<String, String> properties = new HashMap<>();
        properties.put("Successful Verification", "true");
        properties.put("Failure Node Number", "2");
        getClientUtil().updateReportingTask(reportingTask, properties);

        final List<ConfigVerificationResultDTO> resultList = getClientUtil().verifyReportingTaskConfig(reportingTask.getId(), properties);
        assertEquals(3, resultList.size());

        // First verification result will be component validation.
        assertEquals(Outcome.SUCCESSFUL.name(), resultList.get(0).getOutcome());
        // Second verification result will be verification results
        assertEquals(Outcome.SUCCESSFUL.name(), resultList.get(1).getOutcome());
        // Third verification result is for Fail On Primary Node
        // assertEquals(Outcome.FAILED.name(), resultList.get(2).getOutcome());  // NIFI-9717
    }

}
