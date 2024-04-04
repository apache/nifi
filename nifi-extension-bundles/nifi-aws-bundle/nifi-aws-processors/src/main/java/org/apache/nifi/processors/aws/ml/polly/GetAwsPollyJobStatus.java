/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.processors.aws.ml.polly;

import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor;
import software.amazon.awssdk.services.polly.PollyClient;
import software.amazon.awssdk.services.polly.PollyClientBuilder;
import software.amazon.awssdk.services.polly.model.GetSpeechSynthesisTaskRequest;
import software.amazon.awssdk.services.polly.model.GetSpeechSynthesisTaskResponse;
import software.amazon.awssdk.services.polly.model.TaskStatus;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Polly"})
@CapabilityDescription("Retrieves the current status of an AWS Polly job.")
@SeeAlso({StartAwsPollyJob.class})
@WritesAttributes({
        @WritesAttribute(attribute = "PollyS3OutputBucket", description = "The bucket name where polly output will be located."),
        @WritesAttribute(attribute = "filename", description = "Object key of polly output."),
        @WritesAttribute(attribute = "outputLocation", description = "S3 path-style output location of the result.")
})
public class GetAwsPollyJobStatus extends AbstractAwsMachineLearningJobStatusProcessor<PollyClient, PollyClientBuilder> {
    private static final String BUCKET = "bucket";
    private static final String KEY = "key";
    private static final Pattern S3_PATH = Pattern.compile("https://s3.*amazonaws.com/(?<" + BUCKET + ">[^/]+)/(?<" + KEY + ">.*)");
    private static final String AWS_S3_BUCKET = "PollyS3OutputBucket";
    private static final String AWS_S3_KEY = "filename";

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> parentRelationships = new HashSet<>(super.getRelationships());
        parentRelationships.remove(REL_THROTTLED);
        return Set.copyOf(parentRelationships);
    }

    @Override
    protected PollyClientBuilder createClientBuilder(final ProcessContext context) {
        return PollyClient.builder();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final GetSpeechSynthesisTaskResponse speechSynthesisTask;
        try {
            speechSynthesisTask = getSynthesisTask(context, flowFile);
        } catch (final Exception e) {
            getLogger().warn("Failed to get Polly Job status", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final TaskStatus taskStatus = speechSynthesisTask.synthesisTask().taskStatus();

        if (taskStatus == TaskStatus.IN_PROGRESS || taskStatus == TaskStatus.SCHEDULED) {
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_RUNNING);
        } else if (taskStatus == TaskStatus.COMPLETED) {
            final String outputUri = speechSynthesisTask.synthesisTask().outputUri();

            final Matcher matcher = S3_PATH.matcher(outputUri);
            if (matcher.find()) {
                flowFile = session.putAttribute(flowFile, AWS_S3_BUCKET, matcher.group(BUCKET));
                flowFile = session.putAttribute(flowFile, AWS_S3_KEY, matcher.group(KEY));
            }
            FlowFile childFlowFile = session.create(flowFile);
            childFlowFile = writeToFlowFile(session, childFlowFile, speechSynthesisTask);
            childFlowFile = session.putAttribute(childFlowFile, AWS_TASK_OUTPUT_LOCATION, outputUri);
            session.transfer(flowFile, REL_ORIGINAL);
            session.transfer(childFlowFile, REL_SUCCESS);
            getLogger().info("Amazon Polly Task Completed {}", flowFile);
        } else if (taskStatus == TaskStatus.FAILED) {
            final String failureReason =  speechSynthesisTask.synthesisTask().taskStatusReason();
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, failureReason);
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Amazon Polly Task Failed {} Reason [{}]", flowFile, failureReason);
        } else if (taskStatus == TaskStatus.UNKNOWN_TO_SDK_VERSION) {
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, "Unrecognized job status");
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Amazon Polly Task Failed {} Reason [Unrecognized job status]", flowFile);
        }
    }

    private GetSpeechSynthesisTaskResponse getSynthesisTask(final ProcessContext context, final FlowFile flowFile) {
        final String taskId = context.getProperty(TASK_ID).evaluateAttributeExpressions(flowFile).getValue();
        final GetSpeechSynthesisTaskRequest request = GetSpeechSynthesisTaskRequest.builder().taskId(taskId).build();
        return getClient(context).getSpeechSynthesisTask(request);
    }
}
