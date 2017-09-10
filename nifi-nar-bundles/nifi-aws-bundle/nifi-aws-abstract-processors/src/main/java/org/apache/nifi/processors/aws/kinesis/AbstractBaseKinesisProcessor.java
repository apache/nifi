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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;

import com.amazonaws.AmazonWebServiceClient;

/**
 * This class provides processor the base class for kinesis client
 */
public abstract class AbstractBaseKinesisProcessor<ClientType extends AmazonWebServiceClient>
    extends AbstractAWSCredentialsProviderProcessor<ClientType> {

    /**
     * Kinesis put record response error message
     */
    public static final String AWS_KINESIS_ERROR_MESSAGE = "aws.kinesis.error.message";

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .displayName("Message Batch Size")
            .name("message-batch-size")
            .description("Batch size for messages (1-500).")
            .defaultValue("250")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 500, true))
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_MESSAGE_BUFFER_SIZE_MB = new PropertyDescriptor.Builder()
            .name("max-message-buffer-size")
            .displayName("Max message buffer size (MB)")
            .description("Max message buffer size in Mega-bytes")
            .defaultValue("1 MB")
            .required(false)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .sensitive(false)
            .build();

    /**
     * Max buffer size 1 MB
     */
    public static final int MAX_MESSAGE_SIZE = 1000 * 1024;

    protected FlowFile handleFlowFileTooBig(final ProcessSession session, FlowFile flowFileCandidate,
            final String streamName, String message) {
        flowFileCandidate = session.putAttribute(flowFileCandidate, message,
            "record too big " + flowFileCandidate.getSize() + " max allowed " + MAX_MESSAGE_SIZE );
        session.transfer(flowFileCandidate, REL_FAILURE);
        getLogger().error("Failed to publish to kinesis {} records {} because the size was greater than {} bytes",
            new Object[]{streamName, flowFileCandidate, MAX_MESSAGE_SIZE});
        return flowFileCandidate;
    }

    protected List<FlowFile> filterMessagesByMaxSize(final ProcessSession session, final int batchSize, final long maxBufferSizeBytes, final String streamName, String message) {
        List<FlowFile> flowFiles = new ArrayList<FlowFile>(batchSize);

        long currentBufferSizeBytes = 0;

        for (int i = 0; (i < batchSize) && (currentBufferSizeBytes <= maxBufferSizeBytes); i++) {

            FlowFile flowFileCandidate = session.get();
            if ( flowFileCandidate == null )
                break;

            if (flowFileCandidate.getSize() > MAX_MESSAGE_SIZE) {
                flowFileCandidate = handleFlowFileTooBig(session, flowFileCandidate, streamName, message);
                continue;
            }

            currentBufferSizeBytes += flowFileCandidate.getSize();

            flowFiles.add(flowFileCandidate);
        }
        return flowFiles;
    }
}