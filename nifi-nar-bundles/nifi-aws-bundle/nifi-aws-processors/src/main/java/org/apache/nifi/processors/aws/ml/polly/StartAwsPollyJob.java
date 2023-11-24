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
import org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStarter;
import software.amazon.awssdk.services.polly.PollyClient;
import software.amazon.awssdk.services.polly.PollyClientBuilder;
import software.amazon.awssdk.services.polly.model.StartSpeechSynthesisTaskRequest;
import software.amazon.awssdk.services.polly.model.StartSpeechSynthesisTaskResponse;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Polly"})
@CapabilityDescription("Trigger a AWS Polly job. It should be followed by GetAwsPollyJobStatus processor in order to monitor job status.")
@WritesAttributes({
        @WritesAttribute(attribute = "awsTaskId", description = "The task ID that can be used to poll for Job completion in GetAwsPollyJobStatus")
})
@SeeAlso({GetAwsPollyJobStatus.class})
public class StartAwsPollyJob extends AbstractAwsMachineLearningJobStarter<
        StartSpeechSynthesisTaskRequest, StartSpeechSynthesisTaskRequest.Builder, StartSpeechSynthesisTaskResponse, PollyClient, PollyClientBuilder> {

    @Override
    protected PollyClientBuilder createClientBuilder(final ProcessContext context) {
        return PollyClient.builder();
    }

    @Override
    protected StartSpeechSynthesisTaskResponse sendRequest(final StartSpeechSynthesisTaskRequest request, final ProcessContext context, final FlowFile flowFile) {
        return getClient(context).startSpeechSynthesisTask(request);
    }

    @Override
    protected Class<? extends StartSpeechSynthesisTaskRequest.Builder> getAwsRequestBuilderClass(final ProcessContext context, final FlowFile flowFile) {
        return StartSpeechSynthesisTaskRequest.serializableBuilderClass();
    }

    @Override
    protected String getAwsTaskId(final ProcessContext context, final StartSpeechSynthesisTaskResponse startSpeechSynthesisTaskResponse, final FlowFile flowFile) {
        return startSpeechSynthesisTaskResponse.synthesisTask().taskId();
    }
}
