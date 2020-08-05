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
package org.apache.nifi.toolkit.cli.impl.client.nifi;

import java.io.Closeable;

/**
 * Main interface for interacting with a NiFi instance.
 */
public interface NiFiClient extends Closeable {

    // ----- ControllerClient -----

    ControllerClient getControllerClient();

    ControllerClient getControllerClient(RequestConfig requestConfig);

    // ----- ControllerServicesClient -----

    ControllerServicesClient getControllerServicesClient();

    ControllerServicesClient getControllerServicesClient(RequestConfig requestConfig);

    // ----- FlowClient -----

    FlowClient getFlowClient();

    FlowClient getFlowClient(RequestConfig requestConfig);

    // ----- ProcessGroupClient -----

    ProcessGroupClient getProcessGroupClient();

    ProcessGroupClient getProcessGroupClient(RequestConfig requestConfig);

    // ----- VersionsClient -----

    ProcessorClient getProcessorClient();

    ProcessorClient getProcessorClient(RequestConfig requestConfig);

    // ----- VersionsClient -----

    VersionsClient getVersionsClient();

    VersionsClient getVersionsClient(RequestConfig requestConfig);

    // ----- TenantsClient -----

    TenantsClient getTenantsClient();

    TenantsClient getTenantsClient(RequestConfig requestConfig);

    // ----- PoliciesClient -----

    PoliciesClient getPoliciesClient();

    PoliciesClient getPoliciesClient(RequestConfig requestConfig);

    // ----- TemplatesClient -----

    TemplatesClient getTemplatesClient();

    TemplatesClient getTemplatesClient(RequestConfig requestConfig);

    // ----- ReportingTasksClient -----

    ReportingTasksClient getReportingTasksClient();

    ReportingTasksClient getReportingTasksClient(RequestConfig requestConfig);

    // ----- ParamContextClient -----

    ParamContextClient getParamContextClient();

    ParamContextClient getParamContextClient(RequestConfig requestConfig);

    // ----- ParamContextClient -----

    CountersClient getCountersClient();

    CountersClient getCountersClient(RequestConfig requestConfig);

    // ----- ConnectionClient -----

    ConnectionClient getConnectionClient();

    ConnectionClient getConnectionClient(RequestConfig requestConfig);

    // ----- RemoteProcessGroupClient -----

    RemoteProcessGroupClient getRemoteProcessGroupClient();

    RemoteProcessGroupClient getRemoteProcessGroupClient(RequestConfig requestConfig);

    // ----- InputPortClient -----

    InputPortClient getInputPortClient();

    InputPortClient getInputPortClient(RequestConfig requestConfig);

    // ----- OutputPortClient -----

    OutputPortClient getOutputPortClient();

    OutputPortClient getOutputPortClient(RequestConfig requestConfig);

    // ----- ProvenanceClient -----

    ProvenanceClient getProvenanceClient();

    ProvenanceClient getProvenanceClient(RequestConfig requestConfig);

    // ----- AccessClient -----

    AccessClient getAccessClient();

    /**
     * The builder interface that implementations should provide for obtaining the client.
     */
    interface Builder {

        NiFiClient.Builder config(NiFiClientConfig clientConfig);

        NiFiClientConfig getConfig();

        NiFiClient build();

    }

}
