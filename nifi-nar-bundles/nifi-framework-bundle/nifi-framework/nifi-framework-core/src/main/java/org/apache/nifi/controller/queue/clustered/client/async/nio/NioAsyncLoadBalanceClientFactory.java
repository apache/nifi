package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.queue.clustered.FlowFileContentAccess;
import org.apache.nifi.controller.queue.clustered.client.LoadBalanceFlowFileCodec;
import org.apache.nifi.controller.queue.clustered.client.StandardLoadBalanceFlowFileCodec;
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClientFactory;
import org.apache.nifi.events.EventReporter;

import javax.net.ssl.SSLContext;

public class NioAsyncLoadBalanceClientFactory implements AsyncLoadBalanceClientFactory {
    private final SSLContext sslContext;
    private final int timeoutMillis;
    private final FlowFileContentAccess flowFileContentAccess;
    private final EventReporter eventReporter;
    private final LoadBalanceFlowFileCodec flowFileCodec;

    public NioAsyncLoadBalanceClientFactory(final SSLContext sslContext, final int timeoutMillis, final FlowFileContentAccess flowFileContentAccess, final EventReporter eventReporter,
                                            final LoadBalanceFlowFileCodec loadBalanceFlowFileCodec) {
        this.sslContext = sslContext;
        this.timeoutMillis = timeoutMillis;
        this.flowFileContentAccess = flowFileContentAccess;
        this.eventReporter = eventReporter;
        this.flowFileCodec = loadBalanceFlowFileCodec;
    }


    @Override
    public NioAsyncLoadBalanceClient createClient(final NodeIdentifier nodeIdentifier) {
        return new NioAsyncLoadBalanceClient(nodeIdentifier, sslContext, timeoutMillis, flowFileContentAccess, new StandardLoadBalanceFlowFileCodec(), eventReporter);
    }
}
