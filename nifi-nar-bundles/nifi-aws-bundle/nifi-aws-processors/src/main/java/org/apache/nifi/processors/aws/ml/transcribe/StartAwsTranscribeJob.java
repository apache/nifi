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
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.transcribe.AmazonTranscribeClient;
import com.amazonaws.services.transcribe.model.StartTranscriptionJobRequest;
import com.amazonaws.services.transcribe.model.StartTranscriptionJobResult;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStarter;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Transcribe"})
@CapabilityDescription("Trigger a AWS Transcribe job. It should be followed by GetAwsTranscribeStatus processor in order to monitor job status.")
@SeeAlso({GetAwsTranscribeJobStatus.class})
public class StartAwsTranscribeJob extends AwsMachineLearningJobStarter<AmazonTranscribeClient, StartTranscriptionJobRequest, StartTranscriptionJobResult> {
    @Override
    protected AmazonTranscribeClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonTranscribeClient) AmazonTranscribeClient.builder()
                .withRegion(context.getProperty(REGION).getValue())
                .withCredentials(credentialsProvider)
                .build();
    }

    @Override
    protected AmazonTranscribeClient createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
        return (AmazonTranscribeClient) AmazonTranscribeClient.builder().build();
    }

    @Override
    protected StartTranscriptionJobResult sendRequest(StartTranscriptionJobRequest request, ProcessContext context, FlowFile flowFile) {
        return getClient(context).startTranscriptionJob(request);
    }

    @Override
    protected Class<? extends StartTranscriptionJobRequest> getAwsRequestClass(ProcessContext context, FlowFile flowFile) {
        return StartTranscriptionJobRequest.class;
    }

    @Override
    protected String getAwsTaskId(ProcessContext context, StartTranscriptionJobResult startTranscriptionJobResult, FlowFile flowFile) {
        return startTranscriptionJobResult.getTranscriptionJob().getTranscriptionJobName();
    }
}
