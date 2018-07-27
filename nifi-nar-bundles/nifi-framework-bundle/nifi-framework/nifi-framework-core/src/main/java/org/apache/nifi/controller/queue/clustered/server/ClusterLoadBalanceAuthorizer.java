package org.apache.nifi.controller.queue.clustered.server;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ClusterLoadBalanceAuthorizer implements LoadBalanceAuthorizer {
    private static final Logger logger = LoggerFactory.getLogger(ClusterLoadBalanceAuthorizer.class);

    private final ClusterCoordinator clusterCoordinator;
    private final EventReporter eventReporter;

    public ClusterLoadBalanceAuthorizer(final ClusterCoordinator clusterCoordinator, final EventReporter eventReporter) {
        this.clusterCoordinator = clusterCoordinator;
        this.eventReporter = eventReporter;
    }

    @Override
    public void authorize(final Collection<String> clientIdentities, final Connection connection) throws NotAuthorizedException {
        requireNonNull(connection);

        if (clientIdentities == null) {
            logger.debug("Client DN is null, so assuming that Load Balancing communications are not secure. Authorizing client to add data to Connection with ID {}", connection.getIdentifier());
            return;
        }

        final Set<String> nodeIds = clusterCoordinator.getNodeIdentifiers().stream()
                .map(NodeIdentifier::getApiAddress)
                .collect(Collectors.toSet());

        for (final String clientId : clientIdentities) {
            if (nodeIds.contains(clientId)) {
                logger.debug("Client ID '{}' is in the list of Nodes in the Cluster. Authorizing Client to add data to Connection with ID {}", clientId, connection);
                return;
            }
        }

        final String message = String.format("Authorization failed for Client ID's %s to write to Connection %s because none of the ID's are known Cluster Node Identifiers",
                clientIdentities, connection.getIdentifier());

        logger.warn(message);
        eventReporter.reportEvent(Severity.WARNING, "Load Balanced Connections", message);
        throw new NotAuthorizedException("Client ID's " + clientIdentities + " are not authorized to write data to Connection with ID " + connection.getIdentifier());
    }
}
