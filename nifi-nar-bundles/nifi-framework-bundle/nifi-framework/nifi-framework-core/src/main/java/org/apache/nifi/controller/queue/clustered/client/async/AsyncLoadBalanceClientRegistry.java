package org.apache.nifi.controller.queue.clustered.client.async;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public interface AsyncLoadBalanceClientRegistry {
    void register(String connectionId, NodeIdentifier nodeId, BooleanSupplier emptySupplier, Supplier<FlowFileRecord> flowFileSupplier, TransactionFailureCallback failureCallback,
                  TransactionCompleteCallback successCallback);

    void unregister(String connectionId, NodeIdentifier nodeId);
}
