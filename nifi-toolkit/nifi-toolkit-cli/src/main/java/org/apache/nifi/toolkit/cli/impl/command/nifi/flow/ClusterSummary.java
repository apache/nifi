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
package org.apache.nifi.toolkit.cli.impl.command.nifi.flow;

import org.apache.nifi.toolkit.cli.impl.client.nifi.FlowClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClient;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.toolkit.cli.impl.command.nifi.AbstractNiFiCommand;
import org.apache.nifi.toolkit.cli.impl.result.ClusterSummaryEntityResult;
import org.apache.nifi.web.api.entity.ClusteSummaryEntity;

import java.io.IOException;
import java.util.Properties;

public class ClusterSummary extends AbstractNiFiCommand<ClusterSummaryEntityResult> {
    public ClusterSummary() {
        super("cluster-summary", ClusterSummaryEntityResult.class);
    }

    @Override
    public String getDescription() {
        return "Returns information about the node's cluster summary. " +
                "This provides a way to test if the node is in the expected state.";
    }

    @Override
    public ClusterSummaryEntityResult doExecute(NiFiClient client, Properties properties)
            throws NiFiClientException, IOException {
        final FlowClient flowClient = client.getFlowClient();
        final ClusteSummaryEntity clusterSummary = flowClient.getClusterSummary();
        return new ClusterSummaryEntityResult(getResultType(properties), clusterSummary);
    }
}
