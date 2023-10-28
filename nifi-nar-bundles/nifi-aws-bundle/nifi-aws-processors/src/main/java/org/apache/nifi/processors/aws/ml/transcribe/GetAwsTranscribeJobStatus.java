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

import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStatusProcessor;
import software.amazon.awssdk.services.transcribe.TranscribeClient;
import software.amazon.awssdk.services.transcribe.TranscribeClientBuilder;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobRequest;
import software.amazon.awssdk.services.transcribe.model.GetTranscriptionJobResponse;
import software.amazon.awssdk.services.transcribe.model.LimitExceededException;
import software.amazon.awssdk.services.transcribe.model.TranscriptionJobStatus;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Transcribe"})
@CapabilityDescription("Retrieves the current status of an AWS Transcribe job.")
@SeeAlso({StartAwsTranscribeJob.class})
@WritesAttributes({
        @WritesAttribute(attribute = "outputLocation", description = "S3 path-style output location of the result.")
})
public class GetAwsTranscribeJobStatus extends AbstractAwsMachineLearningJobStatusProcessor<TranscribeClient, TranscribeClientBuilder> {

    @Override
    protected TranscribeClientBuilder createClientBuilder(final ProcessContext context) {
        return TranscribeClient.builder();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            final GetTranscriptionJobResponse job = getJob(context, flowFile);
            final TranscriptionJobStatus status = job.transcriptionJob().transcriptionJobStatus();

            if (TranscriptionJobStatus.COMPLETED == status) {
                flowFile = writeToFlowFile(session, flowFile, job);
                flowFile = session.putAttribute(flowFile, AWS_TASK_OUTPUT_LOCATION, job.transcriptionJob().transcript().transcriptFileUri());
                session.transfer(flowFile, REL_SUCCESS);
            } else if (TranscriptionJobStatus.IN_PROGRESS == status || TranscriptionJobStatus.QUEUED == status) {
                session.transfer(flowFile, REL_RUNNING);
            } else if (TranscriptionJobStatus.FAILED == status) {
                final String failureReason = job.transcriptionJob().failureReason();
                flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, failureReason);
                session.transfer(flowFile, REL_FAILURE);
                getLogger().error("Transcribe Task Failed {} Reason [{}]", flowFile, failureReason);
            } else {
                flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, "Unrecognized job status");
                throw new IllegalStateException("Unrecognized job status");
            }
        } catch (final LimitExceededException e) {
            getLogger().info("Request Rate Limit exceeded", e);
            session.transfer(flowFile, REL_THROTTLED);
        } catch (final Exception e) {
            getLogger().warn("Failed to get Transcribe Job status", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private GetTranscriptionJobResponse getJob(final ProcessContext context, final FlowFile flowFile) {
        final String taskId = context.getProperty(TASK_ID).evaluateAttributeExpressions(flowFile).getValue();
        final GetTranscriptionJobRequest request = GetTranscriptionJobRequest.builder().transcriptionJobName(taskId).build();
        return getClient(context).getTranscriptionJob(request);
    }
}
