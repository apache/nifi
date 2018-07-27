package org.apache.nifi.controller.queue.clustered.partition;

import org.apache.nifi.controller.repository.FlowFileRecord;

public class FirstNodePartitioner implements FlowFilePartitioner {

    @Override
    public QueuePartition getPartition(final FlowFileRecord flowFile, final QueuePartition[] partitions, final QueuePartition localPartition) {
        return partitions[0];
    }

    @Override
    public boolean isRebalanceOnClusterResize() {
        return true;
    }

    @Override
    public boolean isRebalanceOnFailure() {
        return false;
    }

    @Override
    public boolean isPartitionStatic() {
        return true;
    }
}
