package org.apache.nifi.controller.queue.clustered;

import org.apache.nifi.controller.queue.FlowFileQueueContents;
import org.apache.nifi.controller.queue.clustered.partition.FlowFilePartitioner;
import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class MockTransferFailureDestination implements TransferFailureDestination {
    private List<FlowFileRecord> flowFilesTransferred = new ArrayList<>();
    private List<String> swapFilesTransferred = new ArrayList<>();

    @Override
    public void putAll(final Collection<FlowFileRecord> flowFiles, final FlowFilePartitioner partitionerUsed) {
        flowFilesTransferred.addAll(flowFiles);
    }

    public List<FlowFileRecord> getFlowFilesTransferred() {
        return flowFilesTransferred;
    }

    @Override
    public void putAll(final Function<String, FlowFileQueueContents> queueContents, final FlowFilePartitioner partitionerUsed) {
        final FlowFileQueueContents contents = queueContents.apply("unit-test");
        flowFilesTransferred.addAll(contents.getActiveFlowFiles());
        swapFilesTransferred.addAll(contents.getSwapLocations());
    }

    public List<String> getSwapFilesTransferred() {
        return swapFilesTransferred;
    }
}
