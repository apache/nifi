/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.jms.processors.strategy.consumer;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;

import java.util.List;

public class MessageConsumer<T> {

    private final FlowFileWriter<T> flowFileWriter;
    private final AttributeSupplier<T> attributeSupplier;
    private final EventReporter eventReporter;

    private MessageConsumer(FlowFileWriter<T> flowFileWriter, AttributeSupplier<T> attributeSupplier, EventReporter eventReporter) {
        this.flowFileWriter = flowFileWriter;
        this.attributeSupplier = attributeSupplier;
        this.eventReporter = eventReporter;
    }

    public void consumeMessages(ProcessSession session, List<T> messages, MessageConsumerCallback<T> messageConsumerCallback) {
        flowFileWriter.write(session, messages, attributeSupplier, new MessageConsumerCallback<T>() {
            @Override
            public void onSuccess(FlowFile flowFile, List<T> processedMessages, List<T> failedMessages) {
                eventReporter.reportSuccessEvent(session, flowFile);
                messageConsumerCallback.onSuccess(flowFile, processedMessages, failedMessages);
            }

            @Override
            public void onParseFailure(FlowFile flowFile, T message, Exception e) {
                eventReporter.reportParseFailureEvent(session, flowFile);
                messageConsumerCallback.onParseFailure(flowFile, message, e);
            }

            @Override
            public void onFailure(FlowFile flowFile, List<T> processedMessages, List<T> failedMessages, Exception e) {
                eventReporter.reportFailureEvent(session, flowFile);
                messageConsumerCallback.onFailure(flowFile, processedMessages, failedMessages, e);
            }
        });
    }

    public static class Builder<T> {

        private FlowFileWriter<T> flowFileWriter;
        private AttributeSupplier<T> attributeSupplier;
        private EventReporter eventReporter;

        public Builder<T> withFlowFileWriter(FlowFileWriter<T> flowFileWriter) {
            this.flowFileWriter = flowFileWriter;
            return this;
        }

        public Builder<T> withAttributeSupplier(AttributeSupplier<T> attributeSupplier) {
            this.attributeSupplier = attributeSupplier;
            return this;
        }

        public Builder<T> withEventReporter(EventReporter eventReporter) {
            this.eventReporter = eventReporter;
            return this;
        }

        public MessageConsumer<T> build() {
            return new MessageConsumer<>(flowFileWriter, attributeSupplier, eventReporter);
        }
    }
}
