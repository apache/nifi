package org.apache.nifi.controller.queue;

public interface FlowFileQueueFactory {
    FlowFileQueue createFlowFileQueue(LoadBalanceStrategy loadBalanceStrategy, String partitioningAttribute, ConnectionEventListener connectionEventListener);
}
