package org.apache.nifi.controller.queue;

public class StandardLocalQueuePartitionDiagnostics implements LocalQueuePartitionDiagnostics {
    private final FlowFileQueueSize queueSize;
    private final boolean anyPenalized;
    private final boolean allPenalized;

    public StandardLocalQueuePartitionDiagnostics(final FlowFileQueueSize queueSize, final boolean anyPenalized, final boolean allPenalized) {
        this.queueSize = queueSize;
        this.anyPenalized = anyPenalized;
        this.allPenalized = allPenalized;
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

    @Override
    public boolean isAnyActiveFlowFilePenalized() {
        return anyPenalized;
    }

    @Override
    public boolean isAllActiveFlowFilesPenalized() {
        return allPenalized;
    }
}
