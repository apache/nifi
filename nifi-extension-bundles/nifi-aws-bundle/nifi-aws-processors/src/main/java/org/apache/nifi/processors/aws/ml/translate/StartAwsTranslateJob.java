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
import org.apache.nifi.processors.aws.ml.AbstractAwsMachineLearningJobStarter;
import software.amazon.awssdk.services.translate.TranslateClient;
import software.amazon.awssdk.services.translate.TranslateClientBuilder;
import software.amazon.awssdk.services.translate.model.StartTextTranslationJobRequest;
import software.amazon.awssdk.services.translate.model.StartTextTranslationJobResponse;

@Tags({"Amazon", "AWS", "ML", "Machine Learning", "Translate"})
@CapabilityDescription("Trigger a AWS Translate job. It should be followed by GetAwsTranslateJobStatus processor in order to monitor job status.")
@WritesAttributes({
        @WritesAttribute(attribute = "awsTaskId", description = "The task ID that can be used to poll for Job completion in GetAwsTranslateJobStatus")
})
@SeeAlso({GetAwsTranslateJobStatus.class})
public class StartAwsTranslateJob extends AbstractAwsMachineLearningJobStarter<
        StartTextTranslationJobRequest, StartTextTranslationJobRequest.Builder, StartTextTranslationJobResponse, TranslateClient, TranslateClientBuilder> {

    @Override
    protected TranslateClientBuilder createClientBuilder(final ProcessContext context) {
        return TranslateClient.builder();
    }

    @Override
    protected StartTextTranslationJobResponse sendRequest(final StartTextTranslationJobRequest request, final ProcessContext context, final FlowFile flowFile) {
        return getClient(context).startTextTranslationJob(request);
    }

    @Override
    protected Class<? extends StartTextTranslationJobRequest.Builder> getAwsRequestBuilderClass(final ProcessContext context, final FlowFile flowFile) {
        return StartTextTranslationJobRequest.serializableBuilderClass();
    }

    protected String getAwsTaskId(final ProcessContext context, final StartTextTranslationJobResponse startTextTranslationJobResponse, final FlowFile flowFile) {
        return startTextTranslationJobResponse.jobId();
    }
}
