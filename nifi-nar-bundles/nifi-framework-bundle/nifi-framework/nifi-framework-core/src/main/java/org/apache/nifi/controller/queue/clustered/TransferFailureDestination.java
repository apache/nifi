package org.apache.nifi.controller.queue.clustered;

import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.Collection;
import java.util.function.Function;

public interface TransferFailureDestination {
    void putAll(Collection<FlowFileRecord> flowFiles);

    void putAll(Function<String, FlowFileQueueContents> queueContents);
}
