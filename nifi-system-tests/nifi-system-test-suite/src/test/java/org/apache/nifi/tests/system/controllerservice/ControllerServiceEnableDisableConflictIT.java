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

package org.apache.nifi.tests.system.controllerservice;

import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.NodeDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.Collections;

public class ControllerServiceEnableDisableConflictIT extends NiFiSystemIT {
    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        // We need to destroy the environment after each test because otherwise, we have a situation where in the second invocation of
        // #testJoinClusterWithEnabledServiceWhileDisabling when we restart the node, we can have a race condition in which Node 2 is
        // disconnected but hasn't yet acknowledged the disconnection. It's then restarted. Meanwhile, Node 1 keeps attempting to notify
        // Node 2 that it's been disconnected. Once Node 2 is restarted and connects, Node 1 sends it the Disconnection request,
        // and Node 2 then disconnects. This can cause issues during shutdown / cleanup.
        // But if we tear down between tests, this won't occur.
        return true;
    }

    @ParameterizedTest
    @EnumSource(NodeReconnectMode.class)
    public void testJoinClusterWithEnabledServiceWhileDisabling(final NodeReconnectMode reconnectMode) throws NiFiClientException, IOException, InterruptedException {
        final ControllerServiceEntity sleepService = getClientUtil().createControllerService("StandardSleepService");
        getClientUtil().updateControllerServiceProperties(sleepService, Collections.singletonMap("@OnDisabled Sleep Time", "10 secs"));

        getClientUtil().enableControllerService(sleepService);
        final NodeDTO node2Dto = getNodeDtoByNodeIndex(2);
        final String node2Id = node2Dto.getNodeId();

        getClientUtil().disconnectNode(node2Id);
        waitForNodeStatus(node2Dto, "DISCONNECTED");

        switchClientToNode(2);

        getClientUtil().disableControllerService(sleepService);
        switchClientToNode(1);

        switch (reconnectMode) {
            case RECONNECT_DIRECTLY:
                getClientUtil().connectNode(node2Id);
                break;
            case RESTART_NODE:
                final NiFiInstance node2Instance = getNiFiInstance().getNodeInstance(2);
                node2Instance.stop();
                node2Instance.start(true);
                break;
        }

        waitForAllNodesConnected();

        waitFor(() -> {
            final ControllerServiceEntity serviceEntity = getNifiClient().getControllerServicesClient().getControllerService(sleepService.getId());
            return "ENABLED".equalsIgnoreCase(serviceEntity.getComponent().getState());
        });
    }


    private enum NodeReconnectMode {
        RECONNECT_DIRECTLY,
        RESTART_NODE;
    }
}
