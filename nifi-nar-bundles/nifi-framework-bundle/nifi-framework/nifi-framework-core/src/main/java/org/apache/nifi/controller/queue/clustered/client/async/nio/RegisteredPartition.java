package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionCompleteCallback;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionFailureCallback;
import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class RegisteredPartition {
    private final String connectionId;
    private final Supplier<FlowFileRecord> flowFileRecordSupplier;
    private final TransactionFailureCallback failureCallback;
    private final BooleanSupplier emptySupplier;
    private final TransactionCompleteCallback successCallback;
    private final LoadBalanceCompression compression;

    public RegisteredPartition(final String connectionId, final BooleanSupplier emptySupplier, final Supplier<FlowFileRecord> flowFileSupplier, final TransactionFailureCallback failureCallback,
                               final TransactionCompleteCallback successCallback, final LoadBalanceCompression compression) {
        this.connectionId = connectionId;
        this.emptySupplier = emptySupplier;
        this.flowFileRecordSupplier = flowFileSupplier;
        this.failureCallback = failureCallback;
        this.successCallback = successCallback;
        this.compression = compression;
    }

    public boolean isEmpty() {
        return emptySupplier.getAsBoolean();
    }

    public String getConnectionId() {
        return connectionId;
    }

    public Supplier<FlowFileRecord> getFlowFileRecordSupplier() {
        return flowFileRecordSupplier;
    }

    public TransactionFailureCallback getFailureCallback() {
        return failureCallback;
    }

    public TransactionCompleteCallback getSuccessCallback() {
        return successCallback;
    }

    public LoadBalanceCompression getCompression() {
        return compression;
    }
}
