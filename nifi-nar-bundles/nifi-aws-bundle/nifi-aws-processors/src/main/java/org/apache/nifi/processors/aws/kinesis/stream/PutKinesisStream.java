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
package org.apache.nifi.processors.aws.kinesis.stream;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.util.StandardValidators;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "kinesis", "put", "stream"})
@CapabilityDescription("Sends the contents to a specified Amazon Kinesis. "
    + "In order to send data to Kinesis, the stream name has to be specified.")
@WritesAttributes({
    @WritesAttribute(attribute = "aws.kinesis.error.message", description = "Error message on posting message to AWS Kinesis"),
    @WritesAttribute(attribute = "aws.kinesis.error.code", description = "Error code for the message when posting to AWS Kinesis"),
    @WritesAttribute(attribute = "aws.kinesis.sequence.number", description = "Sequence number for the message when posting to AWS Kinesis"),
    @WritesAttribute(attribute = "aws.kinesis.shard.id", description = "Shard id of the message posted to AWS Kinesis")})
public class PutKinesisStream extends AbstractKinesisStreamProcessor {

    /**
     * Kinesis put record response error code
     */
    public static final String AWS_KINESIS_ERROR_CODE = "aws.kinesis.error.code";

    public static final String AWS_KINESIS_SHARD_ID = "aws.kinesis.shard.id";

    public static final String AWS_KINESIS_SEQUENCE_NUMBER = "aws.kinesis.sequence.number";

    public static final PropertyDescriptor KINESIS_PARTITION_KEY = new PropertyDescriptor.Builder()
        .displayName("Amazon Kinesis Stream Partition Key")
        .name("amazon-kinesis-stream-partition-key")
        .description("The partition key attribute.  If it is not set, a random value is used")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${kinesis.partition.key}")
        .required(false)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(KINESIS_STREAM_NAME, KINESIS_PARTITION_KEY, BATCH_SIZE, MAX_MESSAGE_BUFFER_SIZE_MB, REGION, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE,
                AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT, PROXY_CONFIGURATION_SERVICE, PROXY_HOST, PROXY_HOST_PORT, PROXY_USERNAME, PROXY_PASSWORD, ENDPOINT_OVERRIDE));

    /** A random number generator for cases where partition key is not available */
    protected Random randomParitionKeyGenerator = new Random();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {

        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final long maxBufferSizeBytes = context.getProperty(MAX_MESSAGE_BUFFER_SIZE_MB).asDataSize(DataUnit.B).longValue();

        List<FlowFile> flowFiles = filterMessagesByMaxSize(session, batchSize, maxBufferSizeBytes, AWS_KINESIS_ERROR_MESSAGE);

        HashMap<String, List<FlowFile>> hashFlowFiles = new HashMap<>();
        HashMap<String, List<PutRecordsRequestEntry>> recordHash = new HashMap<String, List<PutRecordsRequestEntry>>();

        final AmazonKinesisClient client = getClient();


        try {

            List<FlowFile> failedFlowFiles = new ArrayList<>();
            List<FlowFile> successfulFlowFiles = new ArrayList<>();

            // Prepare batch of records
            for (int i = 0; i < flowFiles.size(); i++) {
                FlowFile flowFile = flowFiles.get(i);

                String streamName = context.getProperty(KINESIS_STREAM_NAME).evaluateAttributeExpressions(flowFile).getValue();;

                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                session.exportTo(flowFile, baos);
                PutRecordsRequestEntry record = new PutRecordsRequestEntry().withData(ByteBuffer.wrap(baos.toByteArray()));

                String partitionKey = context.getProperty(PutKinesisStream.KINESIS_PARTITION_KEY)
                        .evaluateAttributeExpressions(flowFiles.get(i)).getValue();

                if (StringUtils.isBlank(partitionKey) == false) {
                    record.setPartitionKey(partitionKey);
                } else {
                    record.setPartitionKey(Integer.toString(randomParitionKeyGenerator.nextInt()));
                }

                if (recordHash.containsKey(streamName) == false) {
                    recordHash.put(streamName, new ArrayList<>());
                }
                if (hashFlowFiles.containsKey(streamName) == false) {
                    hashFlowFiles.put(streamName, new ArrayList<>());
                }

                hashFlowFiles.get(streamName).add(flowFile);
                recordHash.get(streamName).add(record);
            }

            for (Map.Entry<String, List<PutRecordsRequestEntry>> entryRecord : recordHash.entrySet()) {
                String streamName = entryRecord.getKey();
                List<PutRecordsRequestEntry> records = entryRecord.getValue();

                if (records.size() > 0) {

                    PutRecordsRequest putRecordRequest = new PutRecordsRequest();
                    putRecordRequest.setStreamName(streamName);
                    putRecordRequest.setRecords(records);
                    PutRecordsResult results = client.putRecords(putRecordRequest);

                    List<PutRecordsResultEntry> responseEntries = results.getRecords();
                    for (int i = 0; i < responseEntries.size(); i++ ) {
                        PutRecordsResultEntry entry = responseEntries.get(i);
                        FlowFile flowFile = hashFlowFiles.get(streamName).get(i);

                        Map<String,String> attributes = new HashMap<>();
                        attributes.put(AWS_KINESIS_SHARD_ID, entry.getShardId());
                        attributes.put(AWS_KINESIS_SEQUENCE_NUMBER, entry.getSequenceNumber());

                        if (StringUtils.isBlank(entry.getErrorCode()) == false) {
                            attributes.put(AWS_KINESIS_ERROR_CODE, entry.getErrorCode());
                            attributes.put(AWS_KINESIS_ERROR_MESSAGE, entry.getErrorMessage());
                            flowFile = session.putAllAttributes(flowFile, attributes);
                            failedFlowFiles.add(flowFile);
                        } else {
                            flowFile = session.putAllAttributes(flowFile, attributes);
                            successfulFlowFiles.add(flowFile);
                        }
                    }
                }
                recordHash.get(streamName).clear();
                records.clear();
            }

            if ( failedFlowFiles.size() > 0 ) {
                session.transfer(failedFlowFiles, REL_FAILURE);
                getLogger().error("Failed to publish to kinesis records {}", new Object[]{failedFlowFiles});
            }
            if ( successfulFlowFiles.size() > 0 ) {
                session.transfer(successfulFlowFiles, REL_SUCCESS);
                getLogger().debug("Successfully published to kinesis records {}", new Object[]{successfulFlowFiles});
            }

        } catch (final Exception exception) {
            getLogger().error("Failed to publish due to exception {} flowfiles {} ", new Object[]{exception, flowFiles});
            session.transfer(flowFiles, REL_FAILURE);
            context.yield();
        }
    }
}
