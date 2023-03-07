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

package org.apache.nifi.processors.aws.ml.transcribe;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.textract.model.ThrottlingException;
import com.amazonaws.services.transcribe.AmazonTranscribeClient;
import com.amazonaws.services.transcribe.model.GetTranscriptionJobRequest;
import com.amazonaws.services.transcribe.model.GetTranscriptionJobResult;
import com.amazonaws.services.transcribe.model.TranscriptionJobStatus;
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

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Transcribe"})
@CapabilityDescription("Retrieves the current status of an AWS Transcribe job.")
@SeeAlso({StartAwsTranscribeJob.class})
@WritesAttributes({
        @WritesAttribute(attribute = "outputLocation", description = "S3 path-style output location of the result.")
})
public class GetAwsTranscribeJobStatus extends AwsMachineLearningJobStatusProcessor<AmazonTranscribeClient> {
    @Override
    protected AmazonTranscribeClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonTranscribeClient) AmazonTranscribeClient.builder()
                .withRegion(context.getProperty(REGION).getValue())
                .withCredentials(credentialsProvider)
                .build();
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            GetTranscriptionJobResult job = getJob(context, flowFile);
            TranscriptionJobStatus jobStatus = TranscriptionJobStatus.fromValue(job.getTranscriptionJob().getTranscriptionJobStatus());

            if (TranscriptionJobStatus.COMPLETED == jobStatus) {
                writeToFlowFile(session, flowFile, job);
                session.putAttribute(flowFile, AWS_TASK_OUTPUT_LOCATION, job.getTranscriptionJob().getTranscript().getTranscriptFileUri());
                session.transfer(flowFile, REL_SUCCESS);
            } else if (TranscriptionJobStatus.IN_PROGRESS == jobStatus) {
                session.transfer(flowFile, REL_RUNNING);
            } else if (TranscriptionJobStatus.FAILED == jobStatus) {
                final String failureReason = job.getTranscriptionJob().getFailureReason();
                session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, failureReason);
                session.transfer(flowFile, REL_FAILURE);
                getLogger().error("Transcribe Task Failed {} Reason [{}]", flowFile, failureReason);
            }
        } catch (ThrottlingException e) {
            getLogger().info("Request Rate Limit exceeded", e);
            session.transfer(flowFile, REL_THROTTLED);
            return;
        } catch (Exception e) {
            getLogger().warn("Failed to get Transcribe Job status", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
    }

    private GetTranscriptionJobResult getJob(ProcessContext context, FlowFile flowFile) {
        String taskId = context.getProperty(TASK_ID).evaluateAttributeExpressions(flowFile).getValue();
        GetTranscriptionJobRequest request = new GetTranscriptionJobRequest().withTranscriptionJobName(taskId);
        return getClient(context).getTranscriptionJob(request);
    }
}
