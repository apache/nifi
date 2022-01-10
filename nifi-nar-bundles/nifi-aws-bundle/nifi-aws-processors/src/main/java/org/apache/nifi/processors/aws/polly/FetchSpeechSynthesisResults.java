/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.aws.polly;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.polly.AmazonPollyAsync;
import com.amazonaws.services.polly.AmazonPollyAsyncClient;
import com.amazonaws.services.polly.AmazonPollyAsyncClientBuilder;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskRequest;
import com.amazonaws.services.polly.model.GetSpeechSynthesisTaskResult;
import com.amazonaws.services.polly.model.SynthesisTask;
import com.amazonaws.services.polly.model.TaskStatus;
import com.amazonaws.services.s3.AmazonS3URI;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor;
import org.apache.nifi.processors.aws.s3.FetchS3Object;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

@CapabilityDescription("Fetches the result of an Amazon Polly Speech Synthesis task that was started by the StartSpeechSynthesisTask processor. If the specified task has not yet completed, the " +
    "incoming FlowFile will be routed to the 'in progress' relationship. As a result, it is recommended that the 'in progress' relationship be looped back to this processor in a self-loop. If the " +
    "task has completed successfully, the results of the task will be sent to the 'success' relationship in JSON format. The output FlowFile will also then contain attributes pointing to the " +
    "synthesized speech in Amazon S3, which can be fetched via FetchS3Object. See Processor's Additional Details for more information.")
@WritesAttributes({
    @WritesAttribute(attribute =  "s3.bucket", description = "The S3 bucket that contains the synthesized speech"),
    @WritesAttribute(attribute =  "filename", description = "The S3 key that contains the synthesized speech"),
    @WritesAttribute(attribute =  "s3.region", description = "The S3 region where the synthesized speech was stored"),
    @WritesAttribute(attribute =  "s3.version", description = "The version of the S3 object containing the synthesized speech, if provided by the Amazon Polly service"),
    @WritesAttribute(attribute =  "mime.type", description = "Set to application/json")
})
@Tags({"aws", "cloud", "text", "polly", "ml", "ai", "machine learning", "artificial intelligence", "speech", "text-to-speech", "unstructured"})
@SupportsBatching
@SideEffectFree
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SeeAlso({StartSpeechSynthesisTask.class, FetchS3Object.class})
public class FetchSpeechSynthesisResults extends AbstractAWSCredentialsProviderProcessor<AmazonPollyAsyncClient> {
    private static final String FAILURE_REASON_ATTRIBUTE = "failure.reason";
    private static String JSON_MIME_TYPE = "application/json";

    static final PropertyDescriptor TASK_ID = new PropertyDescriptor.Builder()
        .name("Task ID")
        .displayName("Task ID")
        .description("The ID of the Polly task whose results should be fetched")
        .required(true)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("${aws.polly.task.id}")
        .build();

    private static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(
        TASK_ID,
        AWS_CREDENTIALS_PROVIDER_SERVICE,
        REGION,
        TIMEOUT,
        ENDPOINT_OVERRIDE,
        PROXY_HOST,
        PROXY_HOST_PORT,
        PROXY_USERNAME,
        PROXY_PASSWORD));


    // Relationships
    private static final Relationship REL_ORIGINAL = new Relationship.Builder()
        .name("original")
        .description("Upon successful completion, the original FlowFile will be routed to this relationship while the results of the speech synthesis job will be routed to the 'success' relationship")
        .autoTerminateDefault(true)
        .build();
    private static final Relationship REL_IN_PROGRESS = new Relationship.Builder()
        .name("in progress")
        .description("The Polly speech synthesis job is currently still being processed")
        .build();

    private static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
        REL_ORIGINAL,
        REL_SUCCESS,
        REL_IN_PROGRESS,
        REL_FAILURE
    )));

    private final AtomicReference<ObjectMapper> objectMapperReference = new AtomicReference<>();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected AmazonPollyAsyncClient createClient(final ProcessContext context, final AWSCredentialsProvider credentialsProvider, final ClientConfiguration config) {
        final String region = context.getProperty(REGION).getValue();
        final AmazonPollyAsync client = AmazonPollyAsyncClientBuilder.standard()
            .withClientConfiguration(config)
            .withCredentials(credentialsProvider)
            .withRegion(region)
            .build();

        return (AmazonPollyAsyncClient) client;
    }

    @Override
    protected boolean isInitializeRegionAndEndpoint() {
        return false;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String taskId = context.getProperty(TASK_ID).evaluateAttributeExpressions(flowFile).getValue();

        final GetSpeechSynthesisTaskRequest request = new GetSpeechSynthesisTaskRequest();
        request.setTaskId(taskId);

        final Future<GetSpeechSynthesisTaskResult> resultFuture = client.getSpeechSynthesisTaskAsync(request);

        final GetSpeechSynthesisTaskResult result;
        try {
            try {
                result = resultFuture.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                getLogger().error("Interrupted while waiting for Polly to start speech synthesis job for {}. Will route to failure.", flowFile, e);
                session.transfer(flowFile, REL_FAILURE);
                return;
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        } catch (final Throwable t) {
            getLogger().error("Failed to retrieve Polly speech synthesis results for {}. Routing to failure.", flowFile, t);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final SynthesisTask task = result.getSynthesisTask();
        final TaskStatus status = TaskStatus.fromValue(task.getTaskStatus());
        if (status == TaskStatus.InProgress || status == TaskStatus.Scheduled) {
            session.penalize(flowFile);
            session.transfer(flowFile, REL_IN_PROGRESS);
            return;
        }

        if (status == TaskStatus.Failed) {
            final String failureReason = task.getTaskStatusReason();
            session.putAttribute(flowFile, FAILURE_REASON_ATTRIBUTE, failureReason);
            session.transfer(flowFile, REL_FAILURE);
            getLogger().error("Amazon Polly reported that the task failed for {}: {}", flowFile, failureReason);
            return;
        }

        FlowFile resultsFlowFile = session.create(flowFile);
        try (final OutputStream out = session.write(resultsFlowFile)) {
            final ObjectMapper mapper = getObjectMapper();
            final JsonFactory jsonFactory = new JsonFactory(mapper);

            try (final JsonGenerator jsonGenerator = jsonFactory.createGenerator(out)) {
                jsonGenerator.writeObject(task);
                jsonGenerator.flush();
            }
        } catch (final Exception e) {
            getLogger().error("Failed to write Polly results to {}. Will route original {} to failure", resultsFlowFile, flowFile);
            session.remove(resultsFlowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        // Add attributes to the output FlowFile
        final AmazonS3URI uri = new AmazonS3URI(task.getOutputUri());
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(CoreAttributes.MIME_TYPE.key(), JSON_MIME_TYPE);
        attributes.put("s3.bucket", uri.getBucket());
        attributes.put("filename", uri.getKey());

        if (uri.getRegion() != null) {
            attributes.put("s3.region", uri.getRegion());
        }

        if (uri.getVersionId() != null) {
            attributes.put("s3.version", uri.getVersionId());
        }
        session.putAllAttributes(resultsFlowFile, attributes);

        session.transfer(flowFile, REL_ORIGINAL);
        session.transfer(resultsFlowFile, REL_SUCCESS);

        session.getProvenanceReporter().fetch(resultsFlowFile, "https://polly.amazonaws.com/tasks/" + taskId);
    }

    private ObjectMapper getObjectMapper() {
        // Lazily initialize the ObjectMapper.
        ObjectMapper mapper = objectMapperReference.get();
        if (mapper != null) {
            return mapper;
        }

        mapper = new ObjectMapper();
        objectMapperReference.compareAndSet(null, mapper);
        return mapper;
    }


    @Override
    protected AmazonPollyAsyncClient createClient(final ProcessContext context, final AWSCredentials credentials, final ClientConfiguration config) {
        return null;
    }
}
