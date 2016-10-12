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
package org.apache.nifi.processors.aws.kinesis.streams;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google.common.collect.Lists;
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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.Tuple;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map.Entry;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "streams", "kinesis", "put", "stream"})
@CapabilityDescription("Sends the contents to a specified Amazon Kinesis Streams. "
    + "In order to send data to streams, the streams delivery stream name has to be specified.")
@WritesAttributes({
    @WritesAttribute(attribute = "aws.kinesis.streams.error.message", description = "Error message on posting message to AWS Kinesis streams"),
    @WritesAttribute(attribute = "aws.kinesis.streams.error.code", description = "Error code for the message when posting to AWS Kinesis streams"),
    @WritesAttribute(attribute = "aws.kinesis.streams.record.id", description = "Record id of the message posted to Kinesis streams")})
public class PutKinesisStreams extends AbstractKinesisStreamsProcessor {

    /**
     * Kinesis put record response error message
     */
    public static final String AWS_KINESIS_STREAMS_ERROR_MESSAGE = "aws.kinesis.streams.error.message";

    /**
     * Kinesis put record response error code
     */
    public static final String AWS_KINESIS_STREAMS_ERROR_CODE = "aws.kinesis.streams.error.code";

    /**
     * Kinesis put record response record id
     */
    public static final String AWS_KINESIS_STREAMS_RECORD_ID = "aws.kinesis.streams.record.id";

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(KINESIS_STREAMS_DELIVERY_STREAM_NAME, NR_SHARDS, REGION, ACCESS_KEY, SECRET_KEY, CREDENTIALS_FILE, AWS_CREDENTIALS_PROVIDER_SERVICE, TIMEOUT,
                  PROXY_HOST,PROXY_HOST_PORT));

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

        final int nrShards = context.getProperty(NR_SHARDS).asInteger();
        final String streamsStreamName = context.getProperty(KINESIS_STREAMS_DELIVERY_STREAM_NAME).getValue();

        List<FlowFile> flowFiles = new ArrayList<FlowFile>(nrShards);

        for (int i = 0; (i < nrShards); i++) {

            FlowFile flowFileCandidate = session.get();
            if ( flowFileCandidate == null )
                break;

            if (flowFileCandidate.getSize() > MAX_MESSAGE_SIZE) {
                flowFileCandidate = handleFlowFileTooBig(session, flowFileCandidate, streamsStreamName);
                continue;
            }

            flowFiles.add(flowFileCandidate);
        }

        final AmazonKinesisClient client = getClient();

        try {
            List<Tuple<Record, Integer>> recordFlowIdList = new ArrayList<>();

            List<FlowFile> failedFlowFiles = new ArrayList<>();
            List<FlowFile> successfulFlowFiles = new ArrayList<>();

            // Prepare batch of records
            for (int i = 0; i < flowFiles.size(); i++) {
                FlowFile flowFile = flowFiles.get(i);

                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                session.exportTo(flowFile, baos);
                recordFlowIdList.add(new Tuple(new Record().withData(ByteBuffer.wrap(baos.toByteArray())), i));
            }

            if ( recordFlowIdList.size() > 0 ) {
                // Send the batch
                List<PutRecordsRequestEntry> output = new ArrayList<>();
                Map<Integer, List<PutRecordsResult>> responses = new HashMap<>();
                for(Tuple<Record, Integer> recordFlowId: recordFlowIdList) {
                    output.add(new PutRecordsRequestEntry().withPartitionKey(recordFlowId.getValue().toString()).withData(recordFlowId.getKey().getData()));
                    //chunk into 500-record packages
                    List<List<PutRecordsRequestEntry>> recordChuncks = Lists.partition(output, 500);
                    List<PutRecordsResult> imr = new ArrayList<>();
                    for (List<PutRecordsRequestEntry> chunk : recordChuncks) {
                        PutRecordsRequest request = new PutRecordsRequest()
                                .withStreamName(streamsStreamName)
                                .withRecords(chunk);
                        imr.add(client.putRecords(request));
                    }
                    responses.put(recordFlowId.getValue(), imr);
                }

                // Separate out the successful and failed flow files per response
                for (Entry<Integer, List<PutRecordsResult>> response : responses.entrySet()) {
                    FlowFile flowFile = flowFiles.get(response.getKey());
                    Boolean addToFailed = false;
                    for (PutRecordsResult res: response.getValue()) {
                        Boolean failures = res.getFailedRecordCount() > 0;
                        Map<String,String> attributes = new HashMap<>();
                        for (PutRecordsResultEntry r : res.getRecords()) {
                            attributes.put(AWS_KINESIS_STREAMS_RECORD_ID, r.getSequenceNumber());
                            if (failures) {
                                addToFailed = true;
                                if ( ! StringUtils.isBlank(r.getErrorCode()) ) {
                                    attributes.put(AWS_KINESIS_STREAMS_ERROR_CODE, r.getErrorCode());
                                    attributes.put(AWS_KINESIS_STREAMS_ERROR_MESSAGE, r.getErrorMessage());
                                }
                            }
                            flowFile = session.putAllAttributes(flowFile, attributes);
                        }
                    }
                    if (addToFailed) {
                        failedFlowFiles.add(flowFile);
                    } else {
                        successfulFlowFiles.add(flowFile);
                    }
                }
                if ( failedFlowFiles.size() > 0 ) {
                    session.transfer(failedFlowFiles, REL_FAILURE);
                    getLogger().error("Failed to publish to kinesis streams {} records {}", new Object[]{streamsStreamName, failedFlowFiles});
                }
                if ( successfulFlowFiles.size() > 0 ) {
                    session.transfer(successfulFlowFiles, REL_SUCCESS);
                    getLogger().info("Successfully published to kinesis streams {} records {}", new Object[]{streamsStreamName, successfulFlowFiles});
                }
                recordFlowIdList.clear();
            }

        } catch (final Exception exception) {
            getLogger().error("Failed to publish to kinesis streams {} with exception {}", new Object[]{flowFiles, exception});
            session.transfer(flowFiles, REL_FAILURE);
            context.yield();
        }
    }

    protected FlowFile handleFlowFileTooBig(final ProcessSession session, FlowFile flowFileCandidate,
            final String streamsStreamName) {
        flowFileCandidate = session.putAttribute(flowFileCandidate, AWS_KINESIS_STREAMS_ERROR_MESSAGE,
            "record too big " + flowFileCandidate.getSize() + " max allowed " + MAX_MESSAGE_SIZE );
        session.transfer(flowFileCandidate, REL_FAILURE);
        getLogger().error("Failed to publish to kinesis streams {} records {} because the size was greater than {} bytes",
            new Object[]{streamsStreamName, flowFileCandidate, MAX_MESSAGE_SIZE});
        return flowFileCandidate;
    }

}
