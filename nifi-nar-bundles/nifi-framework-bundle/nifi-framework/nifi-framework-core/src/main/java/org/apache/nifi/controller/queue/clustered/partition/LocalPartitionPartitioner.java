package org.apache.nifi.controller.queue.clustered.partition;

import org.apache.nifi.controller.repository.FlowFileRecord;

public class LocalPartitionPartitioner implements FlowFilePartitioner {
    @Override
    public QueuePartition getPartition(final FlowFileRecord flowFile, final QueuePartition[] partitions, final QueuePartition localPartition) {
        return localPartition;
    }

    @Override
    public boolean isRebalanceOnClusterResize() {
        return false;
    }

    @Override
    public boolean isRebalanceOnFailure() {
        return false;
    }

}
