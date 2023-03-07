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
import com.amazonaws.services.translate.model.StartTextTranslationJobRequest;
import com.amazonaws.services.translate.model.StartTextTranslationJobResult;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.aws.ml.AwsMachineLearningJobStarter;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Translate"})
@CapabilityDescription("Trigger a AWS Translate job. It should be followed by GetAwsTranslateJobStatus processor in order to monitor job status.")
@SeeAlso({GetAwsTranslateJobStatus.class})
public class StartAwsTranslateJob extends AwsMachineLearningJobStarter<AmazonTranslateClient, StartTextTranslationJobRequest, StartTextTranslationJobResult> {
    @Override
    protected AmazonTranslateClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        return (AmazonTranslateClient) AmazonTranslateClient.builder()
                .withRegion(context.getProperty(REGION).getValue())
                .withCredentials(credentialsProvider)
                .build();
    }

    @Override
    protected StartTextTranslationJobResult sendRequest(StartTextTranslationJobRequest request, ProcessContext context, FlowFile flowFile) {
        return getClient(context).startTextTranslationJob(request);
    }

    @Override
    protected Class<StartTextTranslationJobRequest> getAwsRequestClass(ProcessContext context, FlowFile flowFile) {
        return StartTextTranslationJobRequest.class;
    }

    protected String getAwsTaskId(ProcessContext context, StartTextTranslationJobResult startTextTranslationJobResult, FlowFile flowFile) {
        return startTextTranslationJobResult.getJobId();
    }
}
