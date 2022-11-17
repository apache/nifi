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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.translate.AmazonTranslateClient;
import com.amazonaws.services.translate.model.DescribeTextTranslationJobRequest;
import com.amazonaws.services.translate.model.DescribeTextTranslationJobResult;
import com.amazonaws.services.translate.model.JobStatus;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStatusGetter;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Translate"})
@CapabilityDescription("Retrieves the current status of an AWS Translate job.")
@SeeAlso({StartAwsTranslateJob.class})
public class GetAwsTranslateJobStatus extends AwsMachineLearningJobStatusGetter<AmazonTranslateClient> {
    @Override
    protected AmazonTranslateClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonTranslateClient) AmazonTranslateClient.builder()
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
        String awsTaskId = flowFile.getAttribute(AWS_TASK_ID_PROPERTY);
        DescribeTextTranslationJobResult describeTextTranslationJobResult = getStatusString(awsTaskId);
        JobStatus status = JobStatus.fromValue(describeTextTranslationJobResult.getTextTranslationJobProperties().getJobStatus());

        if (status == JobStatus.IN_PROGRESS || status == JobStatus.SUBMITTED) {
            writeToFlowFile(session, flowFile, describeTextTranslationJobResult);
            session.penalize(flowFile);
            session.transfer(flowFile, REL_RUNNING);
        }

        if (status == JobStatus.COMPLETED) {
            session.putAttribute(flowFile, AWS_TASK_OUTPUT_LOCATION, describeTextTranslationJobResult.getTextTranslationJobProperties().getOutputDataConfig().getS3Uri());
            writeToFlowFile(session, flowFile, describeTextTranslationJobResult);
            session.transfer(flowFile, REL_SUCCESS);
        }

        if (status == JobStatus.FAILED || status == JobStatus.COMPLETED_WITH_ERROR) {
            writeToFlowFile(session, flowFile, describeTextTranslationJobResult);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private DescribeTextTranslationJobResult getStatusString(String awsTaskId) {
        DescribeTextTranslationJobRequest request = new DescribeTextTranslationJobRequest().withJobId(awsTaskId);
        DescribeTextTranslationJobResult translationJobsResult = getClient().describeTextTranslationJob(request);
        return translationJobsResult;
    }
}
