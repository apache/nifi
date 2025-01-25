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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExceptionInFlowFileFilterIT extends NiFiSystemIT {

    @Test
    public void testFlowFilesRemainAccessible() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity throwException = getClientUtil().createProcessor("ThrowExceptionInFlowFileFilter");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        final ConnectionEntity generateToException = getClientUtil().createConnection(generate, throwException, "success");
        final ConnectionEntity exceptionToTerminate = getClientUtil().createConnection(throwException, terminate, "success");

        getClientUtil().waitForValidProcessor(generate.getId());
        getClientUtil().waitForValidProcessor(throwException.getId());

        getClientUtil().startProcessor(generate);
        waitForQueueCount(generateToException, 1);
        getClientUtil().startProcessor(throwException);

        Thread.sleep(500L);
        getClientUtil().stopProcessor(throwException);

        getClientUtil().updateProcessorProperties(throwException, Map.of("Throw Exception", "false"));
        getClientUtil().waitForValidProcessor(throwException.getId());

        assertEquals(1, getConnectionQueueSize(generateToException.getId()));
        getClientUtil().startProcessor(throwException);

        waitForQueueCount(exceptionToTerminate, 1);
    }

}
