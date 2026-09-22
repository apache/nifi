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

package org.apache.nifi.tests.system.registry;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

class RegistryClientRestartIT extends NiFiSystemIT {
    private static final long VERSION_CONTROL_SYNC_TIMEOUT_SECONDS = 90;

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Test
    void testVersionControlledProcessGroupSynchronizesAfterRestart() throws Exception {
        final FlowRegistryClientEntity registryClient = registerClient();
        final ProcessGroupEntity processGroup = getClientUtil().createProcessGroup("Versioned Process Group", "root");
        getClientUtil().createProcessor("TerminateFlowFile", processGroup.getId());
        getClientUtil().startVersionControl(processGroup, registryClient, RegistryClientIT.TEST_FLOWS_BUCKET, "restart-flow");

        waitForVersionControlState(processGroup.getId(), VersionControlInformationDTO.UP_TO_DATE);

        getNiFiInstance().stop();
        getNiFiInstance().start();
        setupClient();

        waitForVersionControlState(processGroup.getId(), VersionControlInformationDTO.UP_TO_DATE);
    }

    private void waitForVersionControlState(final String processGroupId, final String expectedState) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(VERSION_CONTROL_SYNC_TIMEOUT_SECONDS);
        String observedState = null;

        while (System.currentTimeMillis() < deadline) {
            observedState = getClientUtil().getVersionControlState(processGroupId);
            if (expectedState.equals(observedState)) {
                return;
            }

            Thread.sleep(100L);
        }

        throw new AssertionError(String.format("Timed out after %d seconds waiting for Process Group [%s] Version Control state [%s]; last observed state [%s]",
                VERSION_CONTROL_SYNC_TIMEOUT_SECONDS, processGroupId, expectedState, observedState));
    }
}
