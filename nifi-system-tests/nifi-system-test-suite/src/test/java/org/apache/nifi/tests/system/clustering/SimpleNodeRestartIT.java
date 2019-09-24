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
package org.apache.nifi.tests.system.clustering;

import org.apache.nifi.tests.system.SpawnedClusterNiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiInstance;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.ClusteSummaryEntity;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class SimpleNodeRestartIT extends NiFiSystemIT {

    @Override
    protected NiFiInstanceFactory getInstanceFactory() {
        return new SpawnedClusterNiFiInstanceFactory(
            "src/test/resources/conf/clustered/node1/bootstrap.conf",
            "src/test/resources/conf/clustered/node2/bootstrap.conf");
    }


    @Test
    public void testRestartNode() throws NiFiClientException, IOException {
        final NiFiInstance secondNode = getNiFiInstance().getNodeInstance(2);
        secondNode.stop();

        secondNode.start();
        waitForAllNodesConnected();

        final ClusteSummaryEntity clusterSummary = getNifiClient().getFlowClient().getClusterSummary();
        assertEquals("2 / 2", clusterSummary.getClusterSummary().getConnectedNodes());
    }
}
