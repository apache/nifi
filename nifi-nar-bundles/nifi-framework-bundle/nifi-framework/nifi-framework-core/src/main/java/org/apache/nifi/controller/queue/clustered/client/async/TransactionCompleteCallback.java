package org.apache.nifi.controller.queue.clustered.client.async;

import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.List;

public interface TransactionCompleteCallback {
    void onTransactionComplete(List<FlowFileRecord> flowFilesSent);
}
