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

import org.apache.nifi.tests.system.NiFiInstanceFactory;

/**
 * Clustered variant of {@link ConnectorTroubleshootingIT}. All Troubleshooting lifecycle tests defined on the parent
 * class are re-executed against a two-node NiFi cluster so that the cluster-replication and node-restart paths are
 * exercised in addition to the standalone paths.
 *
 * <p>This class deliberately does not redefine any of the parent's test methods. The verification of restart-while-in-
 * Troubleshooting and authoritative-flow restoration that already exists in
 * {@link ConnectorTroubleshootingIT#testConfigurationAndAuthoritativeFlowRestoredAfterTroubleshootingRestart()} (and
 * the related restart tests) is the clustered coverage the user requested. Adding the override of
 * {@link #getInstanceFactory()} below is sufficient to run all of those scenarios on a two-node cluster.</p>
 */
public class ClusteredConnectorTroubleshootingIT extends ConnectorTroubleshootingIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }
}
