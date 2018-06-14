package org.apache.nifi.controller.queue;

import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.Collection;

public interface LoadBalancedFlowFileQueue extends FlowFileQueue {
    void receiveFromPeer(Collection<FlowFileRecord> flowFiles);

    void distributeToPartitions(Collection<FlowFileRecord> flowFiles);

    void onTransfer(Collection<FlowFileRecord> flowFiles);

    void onAbort(Collection<FlowFileRecord> flowFiles);
}
