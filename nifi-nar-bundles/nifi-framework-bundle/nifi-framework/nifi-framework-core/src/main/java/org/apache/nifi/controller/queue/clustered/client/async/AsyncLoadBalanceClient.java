package org.apache.nifi.controller.queue.clustered.client.async;

import org.apache.nifi.cluster.protocol.NodeIdentifier;

public interface AsyncLoadBalanceClient {

    NodeIdentifier getNodeIdentifier();

    void start();

    void stop();

}
