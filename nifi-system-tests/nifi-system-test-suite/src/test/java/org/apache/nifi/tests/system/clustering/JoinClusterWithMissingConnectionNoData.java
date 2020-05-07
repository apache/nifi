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

import org.apache.nifi.tests.system.InstanceConfiguration;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.tests.system.SpawnedClusterNiFiInstanceFactory;
import org.junit.Test;

public class JoinClusterWithMissingConnectionNoData extends NiFiSystemIT {
    @Override
    protected NiFiInstanceFactory getInstanceFactory() {
        return new SpawnedClusterNiFiInstanceFactory(
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/clustered/node1/bootstrap.conf")
                .instanceDirectory("target/node1")
                .flowXml("src/test/resources/flows/missing-connection/with-connection.xml.gz")
                .build(),
            new InstanceConfiguration.Builder()
                .bootstrapConfig("src/test/resources/conf/clustered/node2/bootstrap.conf")
                .instanceDirectory("target/node2")
                .flowXml("src/test/resources/flows/missing-connection/without-connection.xml.gz")
                .build()
        );
    }

    @Test
    public void testSuccessfulJoinWithMissingConnectionNoData() {
        // If the node joins then the test is successful.
        waitForAllNodesConnected();
    }

}
