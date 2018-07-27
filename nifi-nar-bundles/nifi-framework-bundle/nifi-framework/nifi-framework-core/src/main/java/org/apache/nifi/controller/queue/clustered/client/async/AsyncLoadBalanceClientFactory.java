package org.apache.nifi.controller.queue.clustered.client.async;

import org.apache.nifi.cluster.protocol.NodeIdentifier;

public interface AsyncLoadBalanceClientFactory {
    AsyncLoadBalanceClient createClient(NodeIdentifier nodeIdentifier);
}
