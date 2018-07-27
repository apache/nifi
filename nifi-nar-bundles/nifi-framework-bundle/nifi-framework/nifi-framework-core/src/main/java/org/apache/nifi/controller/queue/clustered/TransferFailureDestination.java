package org.apache.nifi.controller.queue.clustered;

import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.queue.clustered.partition.FlowFilePartitioner;
import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.Collection;
import java.util.function.Function;

public interface TransferFailureDestination {
    /**
     * Puts all of the given FlowFiles to the appropriate destination queue
     *
     * @param flowFiles the FlowFiles to transfer
     * @param partitionerUsed the partitioner that was used to determine that the given FlowFiles should be grouped together in the first place
     */
    void putAll(Collection<FlowFileRecord> flowFiles, FlowFilePartitioner partitionerUsed);

    /**
     * Puts all of the given FlowFile Queue Contents to the appropriate destination queue
     *
     * @param queueContents
     * @param partitionerUsed the partitioner that was used to determine that the given FlowFiles should be grouped together in the first place
     */
    void putAll(Function<String, FlowFileQueueContents> queueContents, FlowFilePartitioner partitionerUsed);
}
