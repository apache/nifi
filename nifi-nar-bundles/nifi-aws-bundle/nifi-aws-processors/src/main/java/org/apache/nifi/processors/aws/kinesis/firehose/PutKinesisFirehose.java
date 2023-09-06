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
package org.apache.nifi.processors.aws.kinesis.firehose;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.aws.kinesis.KinesisProcessorUtils;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.firehose.FirehoseClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponse;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponseEntry;
import software.amazon.awssdk.services.firehose.model.Record;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "firehose", "kinesis", "put", "stream"})
@CapabilityDescription("Sends the contents to a specified Amazon Kinesis Firehose. "
    + "In order to send data to firehose, the firehose delivery stream name has to be specified.")
@WritesAttributes({
    @WritesAttribute(attribute = "aws.kinesis.firehose.error.message", description = "Error message on posting message to AWS Kinesis Firehose"),
    @WritesAttribute(attribute = "aws.kinesis.firehose.error.code", description = "Error code for the message when posting to AWS Kinesis Firehose"),
    @WritesAttribute(attribute = "aws.kinesis.firehose.record.id", description = "Record id of the message posted to Kinesis Firehose")})
public class PutKinesisFirehose extends AbstractKinesisFirehoseProcessor {

    /**
     * Kinesis put record response error message
     */
    public static final String AWS_KINESIS_FIREHOSE_ERROR_MESSAGE = "aws.kinesis.firehose.error.message";

    /**
     * Kinesis put record response error code
     */
    public static final String AWS_KINESIS_FIREHOSE_ERROR_CODE = "aws.kinesis.firehose.error.code";

    /**
     * Kinesis put record response record id
     */
    public static final String AWS_KINESIS_FIREHOSE_RECORD_ID = "aws.kinesis.firehose.record.id";

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(KINESIS_FIREHOSE_DELIVERY_STREAM_NAME, BATCH_SIZE, MAX_MESSAGE_BUFFER_SIZE_MB, REGION, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT,
                    PROXY_CONFIGURATION_SERVICE, PROXY_HOST, PROXY_HOST_PORT, PROXY_USERNAME, PROXY_PASSWORD, ENDPOINT_OVERRIDE));

    /**
     * Max buffer size 1 MB
     */
    public static final int MAX_MESSAGE_SIZE = 1000 * 1024;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final long maxBufferSizeBytes = context.getProperty(MAX_MESSAGE_BUFFER_SIZE_MB).asDataSize(DataUnit.B).longValue();

        final List<FlowFile> flowFiles = KinesisProcessorUtils.filterMessagesByMaxSize(session, batchSize, maxBufferSizeBytes, AWS_KINESIS_FIREHOSE_ERROR_MESSAGE, getLogger());
        final Map<String, List<FlowFile>> hashFlowFiles = new HashMap<>();
        final Map<String, List<Record>> recordHash = new HashMap<>();

        final FirehoseClient client = getClient(context);

        try {
            final List<FlowFile> failedFlowFiles = new ArrayList<>();
            final List<FlowFile> successfulFlowFiles = new ArrayList<>();

            // Prepare batch of records
            for (int i = 0; i < flowFiles.size(); i++) {
                FlowFile flowFile = flowFiles.get(i);

                final String firehoseStreamName = context.getProperty(KINESIS_FIREHOSE_DELIVERY_STREAM_NAME).evaluateAttributeExpressions(flowFile).getValue();

                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                session.exportTo(flowFile, baos);

                if (recordHash.containsKey(firehoseStreamName) == false) {
                    recordHash.put(firehoseStreamName, new ArrayList<>());
                }

                if (hashFlowFiles.containsKey(firehoseStreamName) == false) {
                    hashFlowFiles.put(firehoseStreamName, new ArrayList<>());
                }

                hashFlowFiles.get(firehoseStreamName).add(flowFile);
                recordHash.get(firehoseStreamName).add(Record.builder().data(SdkBytes.fromByteArray(baos.toByteArray())).build());
            }

            for (final Map.Entry<String, List<Record>> entryRecord : recordHash.entrySet()) {
                final String streamName = entryRecord.getKey();
                final List<Record> records = entryRecord.getValue();

                if (records.size() > 0) {
                    // Send the batch
                    final PutRecordBatchRequest putRecordBatchRequest = PutRecordBatchRequest.builder()
                            .deliveryStreamName(streamName)
                            .records(records)
                            .build();
                    final PutRecordBatchResponse response = client.putRecordBatch(putRecordBatchRequest);

                    // Separate out the successful and failed flow files
                    final List<PutRecordBatchResponseEntry> responseEntries = response.requestResponses();
                    for (int i = 0; i < responseEntries.size(); i++ ) {

                        final PutRecordBatchResponseEntry entry = responseEntries.get(i);
                        FlowFile flowFile = hashFlowFiles.get(streamName).get(i);

                        final Map<String,String> attributes = new HashMap<>();
                        attributes.put(AWS_KINESIS_FIREHOSE_RECORD_ID, entry.recordId());
                        flowFile = session.putAttribute(flowFile, AWS_KINESIS_FIREHOSE_RECORD_ID, entry.recordId());
                        if (StringUtils.isNotBlank(entry.errorCode())) {
                            attributes.put(AWS_KINESIS_FIREHOSE_ERROR_CODE, entry.errorCode());
                            attributes.put(AWS_KINESIS_FIREHOSE_ERROR_MESSAGE, entry.errorMessage());
                            flowFile = session.putAllAttributes(flowFile, attributes);
                            failedFlowFiles.add(flowFile);
                        } else {
                            flowFile = session.putAllAttributes(flowFile, attributes);
                            successfulFlowFiles.add(flowFile);
                        }
                    }
                    recordHash.get(streamName).clear();
                    records.clear();
                }
            }

            if (failedFlowFiles.size() > 0) {
                session.transfer(failedFlowFiles, REL_FAILURE);
                getLogger().error("Failed to publish to kinesis firehose {}", new Object[]{failedFlowFiles});
            }
            if (successfulFlowFiles.size() > 0) {
                session.transfer(successfulFlowFiles, REL_SUCCESS);
                getLogger().info("Successfully published to kinesis firehose {}", new Object[]{successfulFlowFiles});
            }

        } catch (final Exception exception) {
            getLogger().error("Failed to publish to kinesis firehose {} with exception {}", new Object[]{flowFiles, exception});
            session.transfer(flowFiles, REL_FAILURE);
            context.yield();
        }
    }

}
