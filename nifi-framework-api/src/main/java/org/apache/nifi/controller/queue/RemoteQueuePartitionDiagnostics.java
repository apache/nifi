package org.apache.nifi.controller.queue;

public interface RemoteQueuePartitionDiagnostics {
    String getNodeIdentifier();

    QueueSize getUnacknowledgedQueueSize();

    QueueSize getActiveQueueSize();

    QueueSize getSwapQueueSize();

    int getSwapFileCount();
}
