package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClientRegistry;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionCompleteCallback;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionFailureCallback;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class NioAsyncLoadBalanceClientRegistry implements AsyncLoadBalanceClientRegistry {
    private static final Logger logger = LoggerFactory.getLogger(NioAsyncLoadBalanceClientRegistry.class);

    private Map<NodeIdentifier, Set<NioAsyncLoadBalanceClient>> clientMap = new HashMap<>();


    @Override
    public synchronized void register(final String connectionId, final NodeIdentifier nodeId, final BooleanSupplier emptySupplier, final Supplier<FlowFileRecord> flowFileSupplier,
                                      final TransactionFailureCallback failureCallback, final TransactionCompleteCallback successCallback) {
        final Set<NioAsyncLoadBalanceClient> clients = clientMap.get(nodeId);
        if (clients == null) {
            throw new IllegalArgumentException("Cannot register Connection with ID " + connectionId + " to send to Node " + nodeId + " because no Client has been created for that Node ID");
        }

        clients.forEach(client -> client.register(connectionId, emptySupplier, flowFileSupplier, failureCallback, successCallback));
        logger.debug("Registered Connection with ID {} to send to Node {}", connectionId, nodeId);
    }


    @Override
    public synchronized void unregister(final String connectionId, final NodeIdentifier nodeId) {
        final Set<NioAsyncLoadBalanceClient> clients = clientMap.get(nodeId);
        if (clients == null) {
            return;
        }

        clients.forEach(client -> client.unregister(connectionId));
        logger.debug("Un-registered Connection with ID {} so that it will no longer send data to Node {}", connectionId, nodeId);
    }

    public synchronized void addClient(final NioAsyncLoadBalanceClient client) {
        final NodeIdentifier nodeId = client.getNodeIdentifier();
        final Set<NioAsyncLoadBalanceClient> clients = clientMap.computeIfAbsent(nodeId, id -> new HashSet<>());
        clients.add(client);

        logger.debug("Added client {} for communicating with Node {}", client, nodeId);
    }

    public synchronized Set<NioAsyncLoadBalanceClient> getAllClients() {
        return clientMap.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    }

    public synchronized Set<NodeIdentifier> getRegisteredNodeIdentifiers() {
        return new HashSet<>(clientMap.keySet());
    }
}
