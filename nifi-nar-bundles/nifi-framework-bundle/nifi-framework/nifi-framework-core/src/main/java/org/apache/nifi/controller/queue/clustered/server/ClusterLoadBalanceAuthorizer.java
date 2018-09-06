package org.apache.nifi.controller.queue.clustered.server;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterLoadBalanceAuthorizer implements LoadBalanceAuthorizer {
    private static final Logger logger = LoggerFactory.getLogger(ClusterLoadBalanceAuthorizer.class);

    private final ClusterCoordinator clusterCoordinator;
    private final EventReporter eventReporter;

    public ClusterLoadBalanceAuthorizer(final ClusterCoordinator clusterCoordinator, final EventReporter eventReporter) {
        this.clusterCoordinator = clusterCoordinator;
        this.eventReporter = eventReporter;
    }

    @Override
    public void authorize(final Collection<String> clientIdentities) throws NotAuthorizedException {
        if (clientIdentities == null) {
            logger.debug("Client Identities is null, so assuming that Load Balancing communications are not secure. Authorizing client to participate in Load Balancing");
            return;
        }

        final Set<String> nodeIds = clusterCoordinator.getNodeIdentifiers().stream()
                .map(NodeIdentifier::getApiAddress)
                .collect(Collectors.toSet());

        for (final String clientId : clientIdentities) {
            if (nodeIds.contains(clientId)) {
                logger.debug("Client ID '{}' is in the list of Nodes in the Cluster. Authorizing Client to Load Balance data", clientId);
                return;
            }
        }

        final String message = String.format("Authorization failed for Client ID's %s to Load Balance data because none of the ID's are known Cluster Node Identifiers",
                clientIdentities);

        logger.warn(message);
        eventReporter.reportEvent(Severity.WARNING, "Load Balanced Connections", message);
        throw new NotAuthorizedException("Client ID's " + clientIdentities + " are not authorized to Load Balance data");
    }
}
