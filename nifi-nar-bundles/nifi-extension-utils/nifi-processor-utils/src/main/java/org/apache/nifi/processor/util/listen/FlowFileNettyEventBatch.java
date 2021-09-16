package org.apache.nifi.processor.util.listen;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.util.listen.event.NettyEvent;

import java.util.List;

public final class FlowFileNettyEventBatch<E extends NettyEvent> {

        private FlowFile flowFile;
        private List<E> events;

        public FlowFileNettyEventBatch(final FlowFile flowFile, final List<E> events) {
            this.flowFile = flowFile;
            this.events = events;
        }

        public FlowFile getFlowFile() {
            return flowFile;
        }

        public List<E> getEvents() {
            return events;
        }

        public void setFlowFile(FlowFile flowFile) {
            this.flowFile = flowFile;
        }
}
