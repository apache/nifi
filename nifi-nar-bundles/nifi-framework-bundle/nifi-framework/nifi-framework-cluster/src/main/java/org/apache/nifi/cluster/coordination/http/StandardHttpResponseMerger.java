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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.StreamingOutput;

import org.apache.nifi.cluster.coordination.http.endpoints.BulletinBoardEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ComponentStateEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ConnectionStatusEndpiontMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ControllerServiceEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ControllerServiceReferenceEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ControllerServicesEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ControllerStatusEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.CountersEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.DropRequestEndpiontMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.FlowSnippetEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.GroupStatusEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ListFlowFilesEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.PortStatusEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProcessGroupEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProcessorEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProcessorStatusEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProcessorsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProvenanceEventEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ProvenanceQueryEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.RemoteProcessGroupEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.RemoteProcessGroupStatusEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.RemoteProcessGroupsEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ReportingTaskEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.ReportingTasksEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.StatusHistoryEndpointMerger;
import org.apache.nifi.cluster.coordination.http.endpoints.SystemDiagnosticsEndpointMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.StatusMerger;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.node.Node.Status;
import org.apache.nifi.reporting.Bulletin;
import org.apache.nifi.reporting.BulletinQuery;
import org.apache.nifi.reporting.ComponentType;
import org.apache.nifi.stream.io.NullOutputStream;
import org.apache.nifi.web.api.dto.status.ControllerStatusDTO;
import org.apache.nifi.web.api.entity.ControllerStatusEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardHttpResponseMerger implements HttpResponseMerger {
    private Logger logger = LoggerFactory.getLogger(StandardHttpResponseMerger.class);

    private static final int NODE_CONTINUE_STATUS_CODE = 150;
    private final WebClusterManager clusterManager;

    private static final List<EndpointResponseMerger> endpointMergers = new ArrayList<>();
    static {
        endpointMergers.add(new ControllerStatusEndpointMerger());
        endpointMergers.add(new GroupStatusEndpointMerger());
        endpointMergers.add(new ProcessorStatusEndpointMerger());
        endpointMergers.add(new ConnectionStatusEndpiontMerger());
        endpointMergers.add(new PortStatusEndpointMerger());
        endpointMergers.add(new RemoteProcessGroupStatusEndpointMerger());
        endpointMergers.add(new ProcessorEndpointMerger());
        endpointMergers.add(new ProcessorsEndpointMerger());
        endpointMergers.add(new RemoteProcessGroupEndpointMerger());
        endpointMergers.add(new RemoteProcessGroupsEndpointMerger());
        endpointMergers.add(new ProcessGroupEndpointMerger());
        endpointMergers.add(new FlowSnippetEndpointMerger());
        endpointMergers.add(new ProvenanceQueryEndpointMerger());
        endpointMergers.add(new ProvenanceEventEndpointMerger());
        endpointMergers.add(new ControllerServiceEndpointMerger());
        endpointMergers.add(new ControllerServicesEndpointMerger());
        endpointMergers.add(new ControllerServiceReferenceEndpointMerger());
        endpointMergers.add(new ReportingTaskEndpointMerger());
        endpointMergers.add(new ReportingTasksEndpointMerger());
        endpointMergers.add(new DropRequestEndpiontMerger());
        endpointMergers.add(new ListFlowFilesEndpointMerger());
        endpointMergers.add(new ComponentStateEndpointMerger());
        endpointMergers.add(new BulletinBoardEndpointMerger());
        endpointMergers.add(new StatusHistoryEndpointMerger());
        endpointMergers.add(new SystemDiagnosticsEndpointMerger());
        endpointMergers.add(new CountersEndpointMerger());
    }

    public StandardHttpResponseMerger() {
        this(null);
    }

    public StandardHttpResponseMerger(final WebClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    @Override
    public NodeResponse mergeResponses(final URI uri, final String httpMethod, final Set<NodeResponse> nodeResponses) {
        final boolean hasSuccess = hasSuccessfulResponse(nodeResponses);
        if (!hasSuccess) {
            // If we have a response that is a 3xx, 4xx, or 5xx, then we want to choose that.
            // Otherwise, it doesn't matter which one we choose. We do this because if we replicate
            // a mutable request, it's possible that one node will respond with a 409, for instance, while
            // others respond with a 150-Continue. We do not want to pick the 150-Continue; instead, we want
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

        // Choose any of the successful responses to be the 'chosen one'.
        final NodeResponse clientResponse = successResponses.iterator().next();

        final EndpointResponseMerger merger = getEndpointResponseMerger(uri, httpMethod);
        if (merger == null) {
            return clientResponse;
        }

        final NodeResponse response = merger.merge(uri, httpMethod, successResponses, problematicResponses, clientResponse);
        if (clusterManager != null) {
            mergeNCMBulletins(response, uri, httpMethod);
        }

        return response;
    }

    /**
     * This method merges bulletins from the NCM. Eventually, the NCM will go away entirely, and
     * at that point, we will completely remove this and the WebClusterManager as a member variable.
     * However, until then, the bulletins from the NCM are important to include, since there is no other
     * node that can include them.
     *
     * @param clientResponse the Node Response that will be returned to the client
     * @param uri the URI
     * @param method the HTTP Method
     *
     * @deprecated this method exists only until we can remove the Cluster Manager from the picture all together. It will then be removed.
     */
    @Deprecated
    private void mergeNCMBulletins(final NodeResponse clientResponse, final URI uri, final String method) {
        // determine if we have at least one response
        final boolean hasClientResponse = clientResponse != null;
        final boolean hasSuccessfulClientResponse = hasClientResponse && clientResponse.is2xx();

        if (hasSuccessfulClientResponse && clusterManager.isControllerStatusEndpoint(uri, method)) {
            // for now, we need to merge the NCM's bulletins too.
            final ControllerStatusEntity responseEntity = (ControllerStatusEntity) clientResponse.getUpdatedEntity();
            final ControllerStatusDTO mergedStatus = responseEntity.getControllerStatus();

            final int totalNodeCount = clusterManager.getNodeIds().size();
            final int connectedNodeCount = clusterManager.getNodeIds(Status.CONNECTED).size();

            final List<Bulletin> ncmControllerBulletins = clusterManager.getBulletinRepository().findBulletinsForController();
            mergedStatus.setBulletins(clusterManager.mergeNCMBulletins(mergedStatus.getBulletins(), ncmControllerBulletins));

            // get the controller service bulletins
            final BulletinQuery controllerServiceQuery = new BulletinQuery.Builder().sourceType(ComponentType.CONTROLLER_SERVICE).build();
            final List<Bulletin> ncmServiceBulletins = clusterManager.getBulletinRepository().findBulletins(controllerServiceQuery);
            mergedStatus.setControllerServiceBulletins(clusterManager.mergeNCMBulletins(mergedStatus.getControllerServiceBulletins(), ncmServiceBulletins));

            // get the reporting task bulletins
            final BulletinQuery reportingTaskQuery = new BulletinQuery.Builder().sourceType(ComponentType.REPORTING_TASK).build();
            final List<Bulletin> ncmReportingTaskBulletins = clusterManager.getBulletinRepository().findBulletins(reportingTaskQuery);
            mergedStatus.setReportingTaskBulletins(clusterManager.mergeNCMBulletins(mergedStatus.getReportingTaskBulletins(), ncmReportingTaskBulletins));

            mergedStatus.setConnectedNodeCount(connectedNodeCount);
            mergedStatus.setTotalNodeCount(totalNodeCount);
            StatusMerger.updatePrettyPrintedFields(mergedStatus);
        }
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

    private static EndpointResponseMerger getEndpointResponseMerger(final URI uri, final String httpMethod) {
        return endpointMergers.stream().filter(p -> p.canHandle(uri, httpMethod)).findFirst().orElse(null);
    }

    private boolean hasSuccessfulResponse(final Set<NodeResponse> allResponses) {
        return allResponses.stream().anyMatch(p -> p.is2xx());
    }


    private void drainResponses(final Set<NodeResponse> responses, final NodeResponse exclude) {
        responses.stream()
            .parallel() // parallelize the draining of the responses, since we have multiple streams to consume
            .filter(response -> response != exclude) // don't include the explicitly excluded node
            .filter(response -> response.getStatus() != NODE_CONTINUE_STATUS_CODE) // don't include any 150-NodeContinue responses because they contain no content
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
