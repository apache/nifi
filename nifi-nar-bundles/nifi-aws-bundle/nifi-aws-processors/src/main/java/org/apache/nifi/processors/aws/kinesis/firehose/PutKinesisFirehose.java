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
import java.util.Collection;
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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
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
            Arrays.asList(KINESIS_FIREHOSE_DELIVERY_STREAM_NAME, MAX_BUFFER_INTERVAL,
                  MAX_BUFFER_SIZE, BATCH_SIZE, REGION, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT,
                  PROXY_HOST,PROXY_HOST_PORT));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));
        final boolean batchSizeSet = validationContext.getProperty(BATCH_SIZE).isSet();

        if ( batchSizeSet) {
           int batchSize = validationContext.getProperty(BATCH_SIZE).asInteger();
           if ( batchSize < 1 || batchSize > 500 ) {
                problems.add(new ValidationResult.Builder().input("Batch Size").valid(false).explanation("Batch size must be between 1 and 500 but was " + batchSize).build());
           }
        }

        final boolean maxBufferIntervalIsSet = validationContext.getProperty(MAX_BUFFER_INTERVAL).isSet();
        if ( maxBufferIntervalIsSet) {
           int maxBufferInterval = validationContext.getProperty(MAX_BUFFER_INTERVAL).asInteger();
           if ( maxBufferInterval < 60 || maxBufferInterval > 900 ) {
                problems.add(new ValidationResult.Builder().input("Max Buffer Interval").valid(false)
                   .explanation("Max Buffer Interval must be between 60 and 900 seconds but was " + maxBufferInterval).build());
           }
        }

        final boolean maxBufferSizeIsSet = validationContext.getProperty(MAX_BUFFER_SIZE).isSet();
        if ( maxBufferSizeIsSet) {
           int maxBufferSize = validationContext.getProperty(MAX_BUFFER_SIZE).asInteger();
           if ( maxBufferSize < 1 || maxBufferSize > 128 ) {
                problems.add(new ValidationResult.Builder().input("Max Buffer Size").valid(false).explanation("Max Buffer Size must be between 1 and 128 (mb) but was " + maxBufferSize).build());
           }
        }
        return problems;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();

        List<FlowFile> flowFiles = session.get(batchSize);
        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }

        final String firehoseStreamName = context.getProperty(KINESIS_FIREHOSE_DELIVERY_STREAM_NAME).getValue();
        final AmazonKinesisFirehoseClient client = getClient();

        try {
            List<Record> records = new ArrayList<>();

            // Prepare batch of records
            for (int i = 0; i < flowFiles.size(); i++) {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                session.exportTo(flowFiles.get(i), baos);
                records.add(new Record().withData(ByteBuffer.wrap(baos.toByteArray())));
            }

            // Send the batch
            PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
            putRecordBatchRequest.setDeliveryStreamName(firehoseStreamName);
            putRecordBatchRequest.setRecords(records);
            PutRecordBatchResult results = client.putRecordBatch(putRecordBatchRequest);

            // Separate out the successful and failed flow files
            List<PutRecordBatchResponseEntry> responseEntries = results.getRequestResponses();
            List<FlowFile> failedFlowFiles = new ArrayList<>();
            List<FlowFile> successfulFlowFiles = new ArrayList<>();
            for (int i = 0; i < responseEntries.size(); i++ ) {
                PutRecordBatchResponseEntry entry = responseEntries.get(i);
                FlowFile flowFile = flowFiles.get(i);

                Map<String,String> attributes = new HashMap<>();
                attributes.put(AWS_KINESIS_FIREHOSE_RECORD_ID, entry.getRecordId());
                flowFile = session.putAttribute(flowFile, AWS_KINESIS_FIREHOSE_RECORD_ID, entry.getRecordId());
                if ( ! StringUtils.isBlank(entry.getErrorCode()) ) {
                    attributes.put(AWS_KINESIS_FIREHOSE_ERROR_CODE, entry.getErrorCode());
                    attributes.put(AWS_KINESIS_FIREHOSE_ERROR_MESSAGE, entry.getErrorMessage());
                    flowFile = session.putAllAttributes(flowFile, attributes);
                    failedFlowFiles.add(flowFile);
                } else {
                    flowFile = session.putAllAttributes(flowFile, attributes);
                    successfulFlowFiles.add(flowFile);
                }
            }

            if ( failedFlowFiles.size() > 0 ) {
                session.transfer(failedFlowFiles, REL_FAILURE);
                getLogger().error("Failed to publish to kinesis firehose {} records {}", new Object[]{firehoseStreamName, failedFlowFiles});
            }

            if ( successfulFlowFiles.size() > 0 ) {
                session.transfer(successfulFlowFiles, REL_SUCCESS);
                getLogger().info("Successfully published to kinesis firehose {} records {}", new Object[]{firehoseStreamName, successfulFlowFiles});
            }

            records.clear();
        } catch (final Exception exception) {
            getLogger().error("Failed to publish to kinesis firehose {} with exception {}", new Object[]{flowFiles, exception});
            session.transfer(flowFiles, REL_FAILURE);
            context.yield();
        }
    }

}
