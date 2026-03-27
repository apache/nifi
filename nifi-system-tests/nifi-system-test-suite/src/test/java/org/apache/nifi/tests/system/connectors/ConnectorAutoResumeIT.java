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

package org.apache.nifi.tests.system.connectors;

import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * System test verifying that a running Connector is not auto-resumed after a restart
 * when the {@code nifi.flowcontroller.autoResumeState} property is set to {@code false}.
 */
public class ConnectorAutoResumeIT extends NiFiSystemIT {

    private static final Logger logger = LoggerFactory.getLogger(ConnectorAutoResumeIT.class);

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Test
    public void testConnectorNotResumedWhenAutoResumeDisabled() throws NiFiClientException, IOException, InterruptedException {
        final ConnectorEntity connector = getClientUtil().createConnector("NopConnector");
        final String connectorId = connector.getId();

        getClientUtil().applyConnectorUpdate(connector);
        getClientUtil().waitForValidConnector(connectorId);

        getClientUtil().startConnector(connectorId);
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        logger.info("Connector {} is RUNNING", connectorId);

        getNiFiInstance().stop();
        logger.info("NiFi stopped");

        getNiFiInstance().setProperty(NiFiProperties.AUTO_RESUME_STATE, "false");
        getNiFiInstance().start();
        setupClient();
        logger.info("NiFi restarted with autoResumeState=false");

        final ConnectorEntity connectorAfterRestart = getNifiClient().getConnectorClient().getConnector(connectorId);
        final String stateAfterRestart = connectorAfterRestart.getComponent().getState();

        assertEquals(ConnectorState.STOPPED.name(), stateAfterRestart);
    }
}
