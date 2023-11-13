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

package org.apache.nifi.processors.aws.ml.translate;

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
import software.amazon.awssdk.services.translate.TranslateClient;
import software.amazon.awssdk.services.translate.TranslateClientBuilder;
import software.amazon.awssdk.services.translate.model.DescribeTextTranslationJobRequest;
import software.amazon.awssdk.services.translate.model.DescribeTextTranslationJobResponse;
import software.amazon.awssdk.services.translate.model.JobStatus;
import software.amazon.awssdk.services.translate.model.LimitExceededException;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Translate"})
@CapabilityDescription("Retrieves the current status of an AWS Translate job.")
@SeeAlso({StartAwsTranslateJob.class})
@WritesAttributes({
        @WritesAttribute(attribute = "outputLocation", description = "S3 path-style output location of the result.")
})
public class GetAwsTranslateJobStatus extends AbstractAwsMachineLearningJobStatusProcessor<TranslateClient, TranslateClientBuilder> {

    @Override
    protected TranslateClientBuilder createClientBuilder(final ProcessContext context) {
        return TranslateClient.builder();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        try {
            final DescribeTextTranslationJobResponse job = getJob(context, flowFile);
            final JobStatus status = job.textTranslationJobProperties().jobStatus();

            flowFile = writeToFlowFile(session, flowFile, job);
            final Relationship transferRelationship;
            String failureReason = null;
            switch (status) {
                case IN_PROGRESS:
                case SUBMITTED:
                case STOP_REQUESTED:
                    flowFile = session.penalize(flowFile);
                    transferRelationship = REL_RUNNING;
                    break;
                case COMPLETED:
                    flowFile = session.putAttribute(flowFile, AWS_TASK_OUTPUT_LOCATION, job.textTranslationJobProperties().outputDataConfig().s3Uri());
                    transferRelationship = REL_SUCCESS;
                    break;
                case FAILED:
                case COMPLETED_WITH_ERROR:
                    failureReason = job.textTranslationJobProperties().message();
                    transferRelationship = REL_FAILURE;
                    break;
                case STOPPED:
                    failureReason = String.format("Job [%s] is stopped", job.textTranslationJobProperties().jobId());
                    transferRelationship = REL_FAILURE;
                    break;
                default:
                    failureReason = "Unknown Job Status";
                    transferRelationship = REL_FAILURE;
            }
            if (failureReason != null) {
                flowFile = session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, failureReason);
            }
            session.transfer(flowFile, transferRelationship);
        } catch (final LimitExceededException e) {
            getLogger().info("Request Rate Limit exceeded", e);
            session.transfer(flowFile, REL_THROTTLED);
        } catch (final Exception e) {
            getLogger().warn("Failed to get Translate Job status", e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private DescribeTextTranslationJobResponse getJob(final ProcessContext context, final FlowFile flowFile) {
        final String taskId = context.getProperty(TASK_ID).evaluateAttributeExpressions(flowFile).getValue();
        final DescribeTextTranslationJobRequest request = DescribeTextTranslationJobRequest.builder().jobId(taskId).build();
        return getClient(context).describeTextTranslationJob(request);
    }
}
