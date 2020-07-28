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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.model.Record;

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

        List<FlowFile> flowFiles = filterMessagesByMaxSize(session, batchSize, maxBufferSizeBytes, AWS_KINESIS_FIREHOSE_ERROR_MESSAGE);
        HashMap<String, List<FlowFile>> hashFlowFiles = new HashMap<>();
        HashMap<String, List<Record>> recordHash = new HashMap<String, List<Record>>();

        final AmazonKinesisFirehoseClient client = getClient();

        try {
            List<FlowFile> failedFlowFiles = new ArrayList<>();
            List<FlowFile> successfulFlowFiles = new ArrayList<>();

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
                recordHash.get(firehoseStreamName).add(new Record().withData(ByteBuffer.wrap(baos.toByteArray())));
            }

            for (Map.Entry<String, List<Record>> entryRecord : recordHash.entrySet()) {
                String streamName = entryRecord.getKey();
                List<Record> records = entryRecord.getValue();

                if (records.size() > 0) {
                    // Send the batch
                    PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
                    putRecordBatchRequest.setDeliveryStreamName(streamName);
                    putRecordBatchRequest.setRecords(records);
                    PutRecordBatchResult results = client.putRecordBatch(putRecordBatchRequest);

                    // Separate out the successful and failed flow files
                    List<PutRecordBatchResponseEntry> responseEntries = results.getRequestResponses();
                    for (int i = 0; i < responseEntries.size(); i++ ) {

                        PutRecordBatchResponseEntry entry = responseEntries.get(i);
                        FlowFile flowFile = hashFlowFiles.get(streamName).get(i);

                        Map<String,String> attributes = new HashMap<>();
                        attributes.put(AWS_KINESIS_FIREHOSE_RECORD_ID, entry.getRecordId());
                        flowFile = session.putAttribute(flowFile, AWS_KINESIS_FIREHOSE_RECORD_ID, entry.getRecordId());
                        if (StringUtils.isBlank(entry.getErrorCode()) == false) {
                            attributes.put(AWS_KINESIS_FIREHOSE_ERROR_CODE, entry.getErrorCode());
                            attributes.put(AWS_KINESIS_FIREHOSE_ERROR_MESSAGE, entry.getErrorMessage());
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
