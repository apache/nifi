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
package org.apache.nifi.cluster.coordination.http;

import org.apache.nifi.cluster.coordination.http.endpoints.AccessPolicyEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.BulletinBoardEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ComponentStateEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ConnectionEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ConnectionStatusEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ConnectionsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ControllerBulletinsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ControllerConfigurationEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ControllerEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ControllerServiceEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ControllerServiceReferenceEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ControllerServiceTypesEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ControllerServicesEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ControllerStatusEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.CountersEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.CurrentUserEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.DropRequestEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.FlowConfigurationEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.FlowMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.FlowSnippetEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.FunnelEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.FunnelsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.GroupStatusEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.InputPortsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.LabelEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.LabelsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ListFlowFilesEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.OutputPortsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.PortEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.PortStatusEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.PrioritizerTypesEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProcessGroupEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProcessGroupsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProcessorDiagnosticsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProcessorEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProcessorStatusEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProcessorTypesEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProcessorsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProvenanceEventEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProvenanceQueryEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.RemoteProcessGroupEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.RemoteProcessGroupStatusEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.RemoteProcessGroupsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ReportingTaskEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ReportingTaskTypesEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ReportingTasksEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.SearchUsersEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.StatusHistoryEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.SystemDiagnosticsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.TemplatesEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.UserEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.UserGroupEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.UserGroupsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.UsersEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.VariableRegistryEndpointMerger;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.stream.io.NullOutputStream;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StandardHttpResponseMapper implements HttpResponseMapper {

    private Logger logger = LoggerFactory.getLogger(StandardHttpResponseMapper.class);

    private final List<EndpointResponseMerger> endpointMergers = new ArrayList<>();

    public StandardHttpResponseMapper(final NiFiProperties nifiProperties) {
        final String snapshotFrequency = nifiProperties.getProperty(NiFiProperties.COMPONENT_STATUS_SNAPSHOT_FREQUENCY, NiFiProperties.DEFAULT_COMPONENT_STATUS_SNAPSHOT_FREQUENCY);
        long snapshotMillis;
        try {
            snapshotMillis = FormatUtils.getTimeDuration(snapshotFrequency, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            snapshotMillis = FormatUtils.getTimeDuration(NiFiProperties.DEFAULT_COMPONENT_STATUS_SNAPSHOT_FREQUENCY, TimeUnit.MILLISECONDS);
        }
        endpointMergers.add(new ControllerStatusEndpointMerger());
        endpointMergers.add(new ControllerBulletinsEndpointMerger());
        endpointMergers.add(new GroupStatusEndpointMerger());
        endpointMergers.add(new ProcessorStatusEndpointMerger());
        endpointMergers.add(new ConnectionStatusEndpointMerger());
        endpointMergers.add(new PortStatusEndpointMerger());
        endpointMergers.add(new RemoteProcessGroupStatusEndpointMerger());
        endpointMergers.add(new ProcessorEndpointMerger());
        endpointMergers.add(new ProcessorsEndpointMerger());
        endpointMergers.add(new ConnectionEndpointMerger());
        endpointMergers.add(new ConnectionsEndpointMerger());
        endpointMergers.add(new PortEndpointMerger());
        endpointMergers.add(new InputPortsEndpointMerger());
        endpointMergers.add(new OutputPortsEndpointMerger());
        endpointMergers.add(new RemoteProcessGroupEndpointMerger());
        endpointMergers.add(new RemoteProcessGroupsEndpointMerger());
        endpointMergers.add(new ProcessGroupEndpointMerger());
        endpointMergers.add(new ProcessGroupsEndpointMerger());
        endpointMergers.add(new FlowSnippetEndpointMerger());
        endpointMergers.add(new ProvenanceQueryEndpointMerger());
        endpointMergers.add(new ProvenanceEventEndpointMerger());
        endpointMergers.add(new ControllerServiceEndpointMerger());
        endpointMergers.add(new ControllerServicesEndpointMerger());
        endpointMergers.add(new ControllerServiceReferenceEndpointMerger());
        endpointMergers.add(new ReportingTaskEndpointMerger());
        endpointMergers.add(new ReportingTasksEndpointMerger());
        endpointMergers.add(new DropRequestEndpointMerger());
        endpointMergers.add(new ListFlowFilesEndpointMerger());
        endpointMergers.add(new ComponentStateEndpointMerger());
        endpointMergers.add(new BulletinBoardEndpointMerger());
        endpointMergers.add(new StatusHistoryEndpointMerger(snapshotMillis));
        endpointMergers.add(new SystemDiagnosticsEndpointMerger());
        endpointMergers.add(new CountersEndpointMerger());
        endpointMergers.add(new FlowMerger());
        endpointMergers.add(new ProcessorTypesEndpointMerger());
        endpointMergers.add(new ControllerServiceTypesEndpointMerger());
        endpointMergers.add(new ReportingTaskTypesEndpointMerger());
        endpointMergers.add(new PrioritizerTypesEndpointMerger());
        endpointMergers.add(new ControllerConfigurationEndpointMerger());
        endpointMergers.add(new CurrentUserEndpointMerger());
        endpointMergers.add(new FlowConfigurationEndpointMerger());
        endpointMergers.add(new TemplatesEndpointMerger());
        endpointMergers.add(new LabelEndpointMerger());
        endpointMergers.add(new LabelsEndpointMerger());
        endpointMergers.add(new FunnelEndpointMerger());
        endpointMergers.add(new FunnelsEndpointMerger());
        endpointMergers.add(new ControllerEndpointMerger());
        endpointMergers.add(new UsersEndpointMerger());
        endpointMergers.add(new UserEndpointMerger());
        endpointMergers.add(new UserGroupsEndpointMerger());
        endpointMergers.add(new UserGroupEndpointMerger());
        endpointMergers.add(new AccessPolicyEndpointMerger());
        endpointMergers.add(new SearchUsersEndpointMerger());
        endpointMergers.add(new VariableRegistryEndpointMerger());
        endpointMergers.add(new ProcessorDiagnosticsEndpointMerger(snapshotMillis));
    }

    @Override
    public NodeResponse mapResponses(final URI uri, final String httpMethod, final Set<NodeResponse> nodeResponses, final boolean merge) {
        final boolean hasSuccess = hasSuccessfulResponse(nodeResponses);
        if (!hasSuccess) {
            // If we have a response that is a 3xx, 4xx, or 5xx, then we want to choose that.
            // Otherwise, it doesn't matter which one we choose. We do this because if we replicate
            // a mutable request, it's possible that one node will respond with a 409, for instance, while
            // others respond with a 202-Accepted. We do not want to pick the 202-Accepted; instead, we want
            // the failed response.
            final NodeResponse clientResponse = nodeResponses.stream().filter(p -> p.getStatus() > 299).findAny().orElse(nodeResponses.iterator().next());

            // Drain the response from all nodes except for the 'chosen one'. This ensures that we don't
            // leave data lingering on the socket and ensures that we don't consume the content of the response
            // that we intend to respond with
            drainResponses(nodeResponses, clientResponse);
            return clientResponse;
        }

        // Determine which responses are successful
        final Set<NodeResponse> successResponses = nodeResponses.stream().filter(p -> p.is2xx()).collect(Collectors.toSet());
        final Set<NodeResponse> problematicResponses = nodeResponses.stream().filter(p -> !p.is2xx()).collect(Collectors.toSet());

        final NodeResponse clientResponse;
        if ("GET".equalsIgnoreCase(httpMethod) && problematicResponses.size() > 0) {
            // If there are problematic responses, at least one of the nodes couldn't complete the request
            clientResponse = problematicResponses.stream().filter(p -> p.getStatus() >= 400 && p.getStatus() < 500).findFirst().orElse(
                    problematicResponses.stream().filter(p -> p.getStatus() > 500).findFirst().orElse(problematicResponses.iterator().next()));
            return clientResponse;
        } else {
            // Choose any of the successful responses to be the 'chosen one'.
            clientResponse = successResponses.iterator().next();
        }

        if (merge == false) {
            return clientResponse;
        }

        EndpointResponseMerger merger = getEndpointResponseMerger(uri, httpMethod);
        if (merger == null) {
            return clientResponse;
        }

        final NodeResponse response = merger.merge(uri, httpMethod, successResponses, problematicResponses, clientResponse);
        return response;
    }

    @Override
    public Set<NodeResponse> getProblematicNodeResponses(final Set<NodeResponse> allResponses) {
        // Check if there are any 2xx responses
        final boolean containsSuccessfulResponse = hasSuccessfulResponse(allResponses);

        if (containsSuccessfulResponse) {
            // If there is a 2xx response, we consider a response to be problematic if it is not 2xx
            return allResponses.stream().filter(p -> !p.is2xx()).collect(Collectors.toSet());
        } else {
            // If no node is successful, we consider a problematic response to be only those that are 5xx
            return allResponses.stream().filter(p -> p.is5xx()).collect(Collectors.toSet());
        }
    }

    @Override
    public boolean isResponseInterpreted(final URI uri, final String httpMethod) {
        return getEndpointResponseMerger(uri, httpMethod) != null;
    }

    private EndpointResponseMerger getEndpointResponseMerger(final URI uri, final String httpMethod) {
        return endpointMergers.stream().filter(p -> p.canHandle(uri, httpMethod)).findFirst().orElse(null);
    }

    private boolean hasSuccessfulResponse(final Set<NodeResponse> allResponses) {
        return allResponses.stream().anyMatch(p -> p.is2xx());
    }

    private void drainResponses(final Set<NodeResponse> responses, final NodeResponse exclude) {
        responses.stream()
                .parallel() // "parallelize" the draining of the responses, since we have multiple streams to consume
                .filter(response -> response != exclude) // don't include the explicitly excluded node
                .filter(response -> response.getStatus() != RequestReplicator.NODE_CONTINUE_STATUS_CODE) // don't include any continue responses because they contain no content
                .forEach(response -> drainResponse(response)); // drain all node responses that didn't get filtered out
    }

    private void drainResponse(final NodeResponse response) {
        if (response.hasThrowable()) {
            return;
        }

        try {
            ((StreamingOutput) response.getResponse().getEntity()).write(new NullOutputStream());
        } catch (final IOException ioe) {
            logger.info("Failed clearing out non-client response buffer from " + response.getNodeId() + " due to: " + ioe, ioe);
        }
    }
}
