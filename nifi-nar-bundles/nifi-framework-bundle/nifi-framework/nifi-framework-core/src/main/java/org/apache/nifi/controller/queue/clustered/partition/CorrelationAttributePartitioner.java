package org.apache.nifi.controller.queue.clustered.partition;

import com.google.common.hash.Hashing;
import org.apache.nifi.controller.repository.FlowFileRecord;

public class CorrelationAttributePartitioner implements FlowFilePartitioner {
    private final String partitioningAttribute;

    public CorrelationAttributePartitioner(final String partitioningAttribute) {
        this.partitioningAttribute = partitioningAttribute;
    }

    @Override
    public QueuePartition getPartition(final FlowFileRecord flowFile, final QueuePartition[] partitions,  final QueuePartition localPartition) {
        final int hash = hash(flowFile);

        // The consistentHash method appears to always return a bucket of '1' if there are 2 possible buckets,
        // so in this case we will just use modulo division to avoid this. I suspect this is a bug with the Guava
        // implementation, but it's not clear at this point.
        final int index;
        if (partitions.length < 3) {
            index = hash % partitions.length;
        } else {
            index = Hashing.consistentHash(hash, partitions.length);
        }

        return partitions[index];
    }

    protected int hash(final FlowFileRecord flowFile) {
        final String partitionAttributeValue = flowFile.getAttribute(partitioningAttribute);
        return (partitionAttributeValue == null) ? 0 : partitionAttributeValue.hashCode();
    }

    @Override
    public boolean isRebalanceOnClusterResize() {
        return true;
    }

    @Override
    public boolean isRebalanceOnFailure() {
        return false;
    }
}
