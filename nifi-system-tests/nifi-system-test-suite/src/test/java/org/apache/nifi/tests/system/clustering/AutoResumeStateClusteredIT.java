/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.tests.system.clustering;

import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class AutoResumeStateClusteredIT extends NiFiSystemIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testRestartWithAutoResumeStateFalse() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().createConnection(generate, terminate, "success");
        getClientUtil().waitForValidProcessor(generate.getId());

        getClientUtil().startProcessor(terminate);
        getClientUtil().waitForRunningProcessor(terminate.getId());

        getClientUtil().startProcessor(generate);
        getClientUtil().waitForRunningProcessor(generate.getId());

        getNiFiInstance().stop();

        getNiFiInstance().getNodeInstance(1).setProperty(NiFiProperties.AUTO_RESUME_STATE, "false");
        getNiFiInstance().getNodeInstance(2).setProperty(NiFiProperties.AUTO_RESUME_STATE, "false");

        getNiFiInstance().start(true);
        waitForAllNodesConnected();

        getClientUtil().waitForStoppedProcessor(terminate.getId());
        getClientUtil().waitForStoppedProcessor(generate.getId());
    }
}
