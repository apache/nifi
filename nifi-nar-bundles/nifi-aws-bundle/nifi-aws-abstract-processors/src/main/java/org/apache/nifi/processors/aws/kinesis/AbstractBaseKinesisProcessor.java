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

import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import com.amazonaws.AmazonWebServiceClient;

/**
 * This class provides processor the base class for kinesis client
 */
public abstract class AbstractBaseKinesisProcessor<ClientType extends AmazonWebServiceClient>
    extends AbstractAWSCredentialsProviderProcessor<ClientType> {
    /**
     * Max buffer size 1 MB
     */
    public static final int MAX_MESSAGE_SIZE = 1000 * 1024;

    private void handleFlowFileTooBig(final ProcessSession session, final FlowFile flowFileCandidate, final String message) {
        final FlowFile tooBig = session.putAttribute(flowFileCandidate, message,
                "record too big " + flowFileCandidate.getSize() + " max allowed " + MAX_MESSAGE_SIZE);
        session.transfer(tooBig, REL_FAILURE);
        getLogger().error("Failed to publish to kinesis records {} because the size was greater than {} bytes",
                tooBig, MAX_MESSAGE_SIZE);
    }

    protected List<FlowFile> filterMessagesByMaxSize(final ProcessSession session, final int batchSize, final long maxBufferSizeBytes, String message) {
        final List<FlowFile> flowFiles = new ArrayList<>(batchSize);

        long currentBufferSizeBytes = 0;
        for (int i = 0; (i < batchSize) && (currentBufferSizeBytes <= maxBufferSizeBytes); i++) {
            final FlowFile flowFileCandidate = session.get();
            if (flowFileCandidate != null) {
                if (flowFileCandidate.getSize() > MAX_MESSAGE_SIZE) {
                    handleFlowFileTooBig(session, flowFileCandidate, message);
                    continue;
                }

                currentBufferSizeBytes += flowFileCandidate.getSize();

                flowFiles.add(flowFileCandidate);
            }
        }
        return flowFiles;
    }
}