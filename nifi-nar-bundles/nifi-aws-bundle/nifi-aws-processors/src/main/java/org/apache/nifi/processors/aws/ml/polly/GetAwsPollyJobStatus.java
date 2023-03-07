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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.polly.AmazonPollyClient;
import com.amazonaws.services.polly.AmazonPollyClientBuilder;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.TaskStatus;
import com.amazonaws.services.textract.model.ThrottlingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusProcessor;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Polly"})
@CapabilityDescription("Retrieves the current status of an AWS Polly job.")
@SeeAlso({StartAwsPollyJob.class})
@WritesAttributes({
        @WritesAttribute(attribute = "PollyS3OutputBucket", description = "The bucket name where polly output will be located."),
        @WritesAttribute(attribute = "PollyS3OutputKey", description = "Object key of polly output."),
        @WritesAttribute(attribute = "outputLocation", description = "S3 path-style output location of the result.")
})
public class GetAwsPollyJobStatus extends AwsMachineLearningJobStatusProcessor<AmazonPollyClient> {
    private static final String BUCKET = "bucket";
    private static final String KEY = "key";
    private static final Pattern S3_PATH = Pattern.compile("https://s3.*amazonaws.com/(?<" + BUCKET + ">[^/]+)/(?<" + KEY + ">.*)");
    private static final String AWS_S3_BUCKET = "PollyS3OutputBucket";
    private static final String AWS_S3_KEY = "filename";

    @Override
    protected AmazonPollyClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonPollyClient) AmazonPollyClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion(context.getProperty(REGION).getValue())
                .build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        GetSpeechSynthesisTaskResult speechSynthesisTask;
        try {
            speechSynthesisTask = getSynthesisTask(context, flowFile);
        } catch (ThrottlingException e) {
            getLogger().info("Request Rate Limit exceeded", e);
            session.transfer(flowFile, REL_THROTTLED);
            return;
        } catch (Exception e) {
            getLogger().warn("Failed to get Polly Job status", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        TaskStatus taskStatus = TaskStatus.fromValue(speechSynthesisTask.getSynthesisTask().getTaskStatus());

        if (taskStatus == TaskStatus.InProgress || taskStatus == TaskStatus.Scheduled) {
            session.penalize(flowFile);
            session.transfer(flowFile, REL_RUNNING);
        } else if (taskStatus == TaskStatus.Completed) {
            String outputUri = speechSynthesisTask.getSynthesisTask().getOutputUri();

            Matcher matcher = S3_PATH.matcher(outputUri);
            if (matcher.find()) {
                session.putAttribute(flowFile, AWS_S3_BUCKET, matcher.group(BUCKET));
                session.putAttribute(flowFile, AWS_S3_KEY, matcher.group(KEY));
            }
            FlowFile childFlowFile = session.create(flowFile);
            writeToFlowFile(session, childFlowFile, speechSynthesisTask);
            childFlowFile = session.putAttribute(childFlowFile, AWS_TASK_OUTPUT_LOCATION, outputUri);
            session.transfer(flowFile, REL_ORIGINAL);
            session.transfer(childFlowFile, REL_SUCCESS);
            getLogger().info("Amazon Polly Task Completed {}", flowFile);
        } else if (taskStatus == TaskStatus.Failed) {
            final String failureReason =  speechSynthesisTask.getSynthesisTask().getTaskStatusReason();
            flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, failureReason);
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Amazon Polly Task Failed {} Reason [{}]", flowFile, failureReason);
        }
    }

    private GetSpeechSynthesisTaskResult getSynthesisTask(ProcessContext context, FlowFile flowFile) {
        String taskId = context.getProperty(TASK_ID).evaluateAttributeExpressions(flowFile).getValue();
        GetSpeechSynthesisTaskRequest request = new GetSpeechSynthesisTaskRequest().withTaskId(taskId);
        return getClient(context).getSpeechSynthesisTask(request);
    }
}
