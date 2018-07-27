package org.apache.nifi.controller.queue;

public class StandardRemoteQueuePartitionDiagnostics implements RemoteQueuePartitionDiagnostics {
    private final String nodeId;
    private final FlowFileQueueSize queueSize;

    public StandardRemoteQueuePartitionDiagnostics(final String nodeId, final FlowFileQueueSize queueSize) {
        this.nodeId = nodeId;
        this.queueSize = queueSize;
    }

    @Override
    public String getNodeIdentifier() {
        return nodeId;
    }

    @Override
    public QueueSize getUnacknowledgedQueueSize() {
        return new QueueSize(queueSize.getUnacknowledgedCount(), queueSize.getUnacknowledgedCount());
    }

    @Override
    public QueueSize getActiveQueueSize() {
        return new QueueSize(queueSize.getActiveCount(), queueSize.getActiveBytes());
    }

    @Override
    public QueueSize getSwapQueueSize() {
        return new QueueSize(queueSize.getSwappedCount(), queueSize.getSwappedBytes());
    }

    @Override
    public int getSwapFileCount() {
        return queueSize.getSwapFileCount();
    }
}
