package org.apache.nifi.controller.repository.metrics;

import org.apache.nifi.controller.repository.FlowFileEvent;

import java.util.Collections;
import java.util.Map;

public class EmptyFlowFileEvent implements FlowFileEvent {
    public static final EmptyFlowFileEvent INSTANCE = new EmptyFlowFileEvent();

    private EmptyFlowFileEvent() {
    }

    @Override
    public int getFlowFilesIn() {
        return 0;
    }

    @Override
    public int getFlowFilesOut() {
        return 0;
    }

    @Override
    public int getFlowFilesRemoved() {
        return 0;
    }

    @Override
    public long getContentSizeIn() {
        return 0;
    }

    @Override
    public long getContentSizeOut() {
        return 0;
    }

    @Override
    public long getContentSizeRemoved() {
        return 0;
    }

    @Override
    public long getBytesRead() {
        return 0;
    }

    @Override
    public long getBytesWritten() {
        return 0;
    }

    @Override
    public long getProcessingNanoseconds() {
        return 0;
    }

    @Override
    public long getAverageLineageMillis() {
        return 0;
    }

    @Override
    public long getAggregateLineageMillis() {
        return 0;
    }

    @Override
    public int getFlowFilesReceived() {
        return 0;
    }

    @Override
    public long getBytesReceived() {
        return 0;
    }

    @Override
    public int getFlowFilesSent() {
        return 0;
    }

    @Override
    public long getBytesSent() {
        return 0;
    }

    @Override
    public int getInvocations() {
        return 0;
    }

    @Override
    public Map<String, Long> getCounters() {
        return Collections.emptyMap();
    }
}
