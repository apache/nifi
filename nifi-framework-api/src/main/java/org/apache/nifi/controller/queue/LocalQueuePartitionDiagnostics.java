package org.apache.nifi.controller.queue;

public interface LocalQueuePartitionDiagnostics {
    QueueSize getUnacknowledgedQueueSize();

    QueueSize getActiveQueueSize();

    QueueSize getSwapQueueSize();

    int getSwapFileCount();

    boolean isAnyActiveFlowFilePenalized();

    boolean isAllActiveFlowFilesPenalized();
}
