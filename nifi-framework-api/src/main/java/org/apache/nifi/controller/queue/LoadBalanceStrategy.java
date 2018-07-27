package org.apache.nifi.controller.queue;

public enum LoadBalanceStrategy {
    /**
     * Do not load balance FlowFiles between nodes in the cluster.
     */
    DO_NOT_LOAD_BALANCE,

    /**
     * Determine which node to send a given FlowFile to based on the value of a user-specified FlowFile Attribute.
     * All FlowFiles that have the same value for said Attribute will be sent to the same node in the cluster.
     */
    PARTITION_BY_ATTRIBUTE,

    /**
     * FlowFiles will be distributed to nodes in the cluster in a Round-Robin fashion.
     */
    ROUND_ROBIN,

    /**
     * All FlowFiles will be sent to the same node. Which node they are sent to is not defined.
     */
    SINGLE_NODE;
}
