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
package org.apache.nifi.tests.system.processor;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RunOnceIT extends NiFiSystemIT {

    @Test
    public void testRunOnce() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        final ConnectionEntity generateToTerminate = getClientUtil().createConnection(generate, terminate, "success");

        getNifiClient().getProcessorClient().runProcessorOnce(generate);
        waitForQueueCount(generateToTerminate.getId(), 1);

        getClientUtil().waitForStoppedProcessor(generate.getId());
        assertEquals(1, getConnectionQueueSize(generateToTerminate.getId()));

        // Test CRON_DRIVEN Strategy
        getClientUtil().updateProcessorSchedulingStrategy(generate, "CRON_DRIVEN");
        getClientUtil().updateProcessorSchedulingPeriod(generate, "* * * * * ?");

        getNifiClient().getProcessorClient().runProcessorOnce(generate);
        waitForQueueCount(generateToTerminate.getId(), 2);

        getClientUtil().waitForStoppedProcessor(generate.getId());
        assertEquals(2, getConnectionQueueSize(generateToTerminate.getId()));
    }
}
