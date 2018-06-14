package org.apache.nifi.controller.queue.clustered.client.async;

import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.List;

public interface TransactionFailureCallback {
    void onTransactionFailed(List<FlowFileRecord> flowFiles, Exception cause, TransactionPhase transactionPhase);

    enum TransactionPhase {
        /**
         * Failure occurred when connecting to the node
         */
        CONNECTING,

        /**
         * Failure occurred when sending data to the node
         */
        SENDING;
    }
}
