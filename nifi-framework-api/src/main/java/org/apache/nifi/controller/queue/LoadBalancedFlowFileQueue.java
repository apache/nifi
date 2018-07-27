package org.apache.nifi.controller.queue;

import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.Collection;

public interface LoadBalancedFlowFileQueue extends FlowFileQueue {
    /**
     * Adds the given FlowFiles to this queue, as they have been received from another node in the cluster
     * @param flowFiles the FlowFiles received from the peer
     */
    void receiveFromPeer(Collection<FlowFileRecord> flowFiles);

    /**
     * Distributes the given FlowFiles to the appropriate partitions. Unlike the {@link #putAll(Collection)} method,
     * this does not alter the size of the FlowFile Queue itself, as it is intended only to place the FlowFiles into
     * their appropriate partitions
     *
     * @param flowFiles the FlowFiles to distribute
     */
    void distributeToPartitions(Collection<FlowFileRecord> flowFiles);

    /**
     * Notifies the queue that the given FlowFiles have been successfully transferred to another node
     * @param flowFiles the FlowFiles that were transferred
     */
    void onTransfer(Collection<FlowFileRecord> flowFiles);

    /**
     * Notifies the queue the a transaction containing the given FlowFiles was aborted
     * @param flowFiles the FlowFiles in the transaction
     */
    void onAbort(Collection<FlowFileRecord> flowFiles);

    /**
     * Handles updating the repositories for the given FlowFiles, which have been expired
     * @param flowFiles the expired FlowFiles
     */
    void handleExpiredRecords(Collection<FlowFileRecord> flowFiles);
}
