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
package org.apache.nifi.processors.aws.kinesis;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.aws.v2.AbstractAwsProcessor;

import java.util.ArrayList;
import java.util.List;

/**
 * This class provides a base for all kinesis processors
 */
public class KinesisProcessorUtils {

    private KinesisProcessorUtils() {

    }

    /**
     * Max buffer size 1 MB
     */
    public static final int MAX_MESSAGE_SIZE = 1000 * 1024;

    /**
     * Filters messages by max size, transferring any flowfiles larger than the max size to Failure.
     * @param session The process session
     * @param batchSize The batch size
     * @param maxBufferSizeBytes The max buffer size in bytes
     * @param errorMessageAttribute The attribute that will contain the error message in case of failure
     * @param logger The component log
     * @return A list of flowfiles that are less than the maximum errorMessageAttribute size
     */
    public static List<FlowFile> filterMessagesByMaxSize(final ProcessSession session, final int batchSize, final long maxBufferSizeBytes,
                                                         final String errorMessageAttribute, final ComponentLog logger) {
        final List<FlowFile> flowFiles = new ArrayList<>();

        long currentBufferSizeBytes = 0;
        for (int i = 0; (i < batchSize) && (currentBufferSizeBytes <= maxBufferSizeBytes); i++) {
            final FlowFile flowFileCandidate = session.get();
            if (flowFileCandidate == null) {
                break;
            }

            if (flowFileCandidate.getSize() > MAX_MESSAGE_SIZE) {
                handleFlowFileTooBig(session, flowFileCandidate, errorMessageAttribute, logger);
                continue;
            }

            currentBufferSizeBytes += flowFileCandidate.getSize();
            flowFiles.add(flowFileCandidate);
        }

        return flowFiles;
    }

    private static void handleFlowFileTooBig(final ProcessSession session, final FlowFile flowFileCandidate, final String message,
                                             final ComponentLog logger) {
        final FlowFile tooBig = session.putAttribute(flowFileCandidate, message,
                "record too big " + flowFileCandidate.getSize() + " max allowed " + MAX_MESSAGE_SIZE);
        session.transfer(tooBig, AbstractAwsProcessor.REL_FAILURE);
        logger.error("Failed to publish to kinesis records {} because the size was greater than {} bytes",
                tooBig, MAX_MESSAGE_SIZE);
    }
}