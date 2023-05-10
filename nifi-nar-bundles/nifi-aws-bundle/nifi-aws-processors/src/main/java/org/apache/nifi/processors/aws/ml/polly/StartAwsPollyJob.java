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
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.StartSpeechSynthesisTaskResult;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStarter;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Polly"})
@CapabilityDescription("Trigger a AWS Polly job. It should be followed by GetAwsPollyJobStatus processor in order to monitor job status.")
@SeeAlso({GetAwsPollyJobStatus.class})
public class StartAwsPollyJob extends AwsMachineLearningJobStarter<AmazonPollyClient, StartSpeechSynthesisTaskRequest, StartSpeechSynthesisTaskResult> {
    @Override
    protected AmazonPollyClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonPollyClient) AmazonPollyClientBuilder.standard()
                .withRegion(context.getProperty(REGION).getValue())
                .withCredentials(credentialsProvider)
                .build();
    }

    @Override
    protected StartSpeechSynthesisTaskResult sendRequest(StartSpeechSynthesisTaskRequest request, ProcessContext context, FlowFile flowFile) {
        return getClient(context).startSpeechSynthesisTask(request);
    }

    @Override
    protected Class<? extends StartSpeechSynthesisTaskRequest> getAwsRequestClass(ProcessContext context, FlowFile flowFile) {
        return StartSpeechSynthesisTaskRequest.class;
    }

    @Override
    protected String getAwsTaskId(ProcessContext context, StartSpeechSynthesisTaskResult startSpeechSynthesisTaskResult, FlowFile flowFile) {
        return startSpeechSynthesisTaskResult.getSynthesisTask().getTaskId();
    }
}
