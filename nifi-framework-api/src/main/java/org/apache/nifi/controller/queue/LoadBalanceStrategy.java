package org.apache.nifi.controller.queue;

public enum LoadBalanceStrategy {
    DO_NOT_LOAD_BALANCE,

    PARTITION_BY_ATTRIBUTE,

    ROUND_ROBIN;
}
