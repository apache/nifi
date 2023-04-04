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
package org.apache.nifi.jms.processors.ioconcept.reader;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.jms.processors.ioconcept.reader.record.RecordSupplier;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.StopWatch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Optional.ofNullable;

public class StateTrackingFlowFileReader implements FlowFileReader {

    public static final String ATTR_READ_FAILED_INDEX_SUFFIX = ".read.failed.index";

    private final String identifier;
    private final RecordSupplier recordSupplier;
    private final ComponentLog logger;

    public StateTrackingFlowFileReader(String identifier, RecordSupplier recordSupplier, ComponentLog logger) {
        this.identifier = identifier;
        this.recordSupplier = recordSupplier;
        this.logger = logger;
    }

    @Override
    public void read(ProcessSession session, FlowFile flowFile, MessageHandler messageHandler, FlowFileReaderCallback flowFileReaderCallback) {
        final StopWatch stopWatch = new StopWatch(true);
        final AtomicInteger processedRecords = new AtomicInteger();

        final String publishFailedIndexAttributeName = identifier + ATTR_READ_FAILED_INDEX_SUFFIX;

        try {
            final Long previousProcessFailedAt = ofNullable(flowFile.getAttribute(publishFailedIndexAttributeName)).map(Long::valueOf).orElse(null);

            session.read(flowFile, in -> recordSupplier.process(flowFile, in, processedRecords, previousProcessFailedAt, logger, messageHandler));

            FlowFile successFlowFile = flowFile;

            final boolean isRecover = previousProcessFailedAt != null;
            if (isRecover) {
                successFlowFile = session.removeAttribute(flowFile, publishFailedIndexAttributeName);
            }

            flowFileReaderCallback.onSuccess(successFlowFile, processedRecords.get(), isRecover, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            logger.error("An error happened while processing records. Routing to failure.", e);

            final FlowFile failedFlowFile = session.putAttribute(flowFile, publishFailedIndexAttributeName, String.valueOf(processedRecords.get()));

            flowFileReaderCallback.onFailure(failedFlowFile, processedRecords.get(), stopWatch.getElapsed(TimeUnit.MILLISECONDS), e);
        }
    }

}
