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
package org.apache.nifi.processors.gcp.pubsub;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.stub.GrpcPublisherStub;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.iam.v1.TestIamPermissionsRequest;
import com.google.iam.v1.TestIamPermissionsResponse;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.pubsub.publish.ContentInputStrategy;
import org.apache.nifi.processors.gcp.pubsub.publish.FlowFileResult;
import org.apache.nifi.processors.gcp.pubsub.publish.MessageTracker;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.PushBackRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StopWatch;
import org.threeten.bp.Duration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MESSAGE_ID_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.MESSAGE_ID_DESCRIPTION;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.TOPIC_NAME_ATTRIBUTE;
import static org.apache.nifi.processors.gcp.pubsub.PubSubAttributes.TOPIC_NAME_DESCRIPTION;

@SeeAlso({ConsumeGCPubSub.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"google", "google-cloud", "gcp", "message", "pubsub", "publish"})
@CapabilityDescription("Publishes the content of the incoming flowfile to the configured Google Cloud PubSub topic. The processor supports dynamic properties." +
        " If any dynamic properties are present, they will be sent along with the message in the form of 'attributes'.")
@DynamicProperty(name = "Attribute name", value = "Value to be set to the attribute",
        description = "Attributes to be set for the outgoing Google Cloud PubSub message", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
@WritesAttributes({
        @WritesAttribute(attribute = MESSAGE_ID_ATTRIBUTE, description = MESSAGE_ID_DESCRIPTION),
        @WritesAttribute(attribute = TOPIC_NAME_ATTRIBUTE, description = TOPIC_NAME_DESCRIPTION)
})
@SystemResourceConsideration(resource = SystemResource.MEMORY, description = "The entirety of the FlowFile's content "
        + "will be read into memory to be sent as a PubSub message.")
public class PublishGCPubSub extends AbstractGCPubSubWithProxyProcessor {
    private static final List<String> REQUIRED_PERMISSIONS = Collections.singletonList("pubsub.topics.publish");
    private static final String TRANSIT_URI_FORMAT_STRING = "gcp://%s";

    public static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("max-batch-size")
            .displayName("Maximum Batch Size")
            .description("Maximum number of FlowFiles processed for each Processor invocation")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .defaultValue("100")
            .build();

    public static final PropertyDescriptor CONTENT_INPUT_STRATEGY = new PropertyDescriptor.Builder()
            .name("content-input-strategy")
            .displayName("Content Input Strategy")
            .description("The strategy used to publish the incoming FlowFile to the Google Cloud PubSub endpoint.")
            .required(true)
            .defaultValue(ContentInputStrategy.FLOWFILE_ORIENTED.getValue())
            .allowableValues(ContentInputStrategy.class)
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("The Record Reader to use for incoming FlowFiles")
            .identifiesControllerService(RecordReaderFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(CONTENT_INPUT_STRATEGY, ContentInputStrategy.RECORD_ORIENTED.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("The Record Writer to use in order to serialize the data before sending to GCPubSub endpoint")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .dependsOn(CONTENT_INPUT_STRATEGY, ContentInputStrategy.RECORD_ORIENTED.getValue())
            .required(true)
            .build();

    public static final PropertyDescriptor MAX_MESSAGE_SIZE = new PropertyDescriptor.Builder()
            .name("max.message.size")
            .displayName("Max Message Size")
            .description("The maximum size of a GCPubSub message in bytes. Defaults to 1 MB (1048576).")
            .dependsOn(CONTENT_INPUT_STRATEGY, ContentInputStrategy.FLOWFILE_ORIENTED.getValue())
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();

    public static final PropertyDescriptor TOPIC_NAME = new PropertyDescriptor.Builder()
            .name("gcp-pubsub-topic")
            .displayName("Topic Name")
            .description("Name of the Google Cloud PubSub Topic")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final Relationship REL_RETRY = new Relationship.Builder()
            .name("retry")
            .description("FlowFiles are routed to this relationship if the Google Cloud Pub/Sub operation fails but attempting the operation again may succeed.")
            .build();

    private Publisher publisher = null;
    private final AtomicReference<Exception> storedException = new AtomicReference<>();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>(super.getSupportedPropertyDescriptors());
        descriptors.add(MAX_BATCH_SIZE);
        descriptors.add(CONTENT_INPUT_STRATEGY);
        descriptors.add(RECORD_READER);
        descriptors.add(RECORD_WRITER);
        descriptors.add(MAX_MESSAGE_SIZE);
        descriptors.add(TOPIC_NAME);
        descriptors.add(BATCH_SIZE_THRESHOLD);
        descriptors.add(BATCH_BYTES_THRESHOLD);
        descriptors.add(BATCH_DELAY_THRESHOLD);
        descriptors.add(PUBSUB_ENDPOINT);
        return Collections.unmodifiableList(descriptors);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .name(propertyDescriptorName)
                .displayName(propertyDescriptorName)
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.unmodifiableSet(
                new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE, REL_RETRY))
        );
    }

    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        try {
            storedException.set(null);
            publisher = getPublisherBuilder(context).build();
        } catch (IOException e) {
            getLogger().error("Failed to create Google Cloud PubSub Publisher due to {}", e);
            storedException.set(e);
        }
    }

    @Override
    public List<ConfigVerificationResult> verify(final ProcessContext context, final ComponentLog verificationLogger, final Map<String, String> attributes) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        Publisher publisher = null;
        try {
            publisher = getPublisherBuilder(context).build();
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Create Publisher")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Successfully created Publisher")
                    .build());
        } catch (final IOException e) {
            verificationLogger.error("Failed to create Publisher", e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Create Publisher")
                    .outcome(Outcome.FAILED)
                    .explanation(String.format("Failed to create Publisher: " + e.getMessage()))
                    .build());
        }

        if (publisher != null) {
            try {
                final PublisherStubSettings publisherStubSettings = PublisherStubSettings.newBuilder()
                        .setCredentialsProvider(FixedCredentialsProvider.create(getGoogleCredentials(context)))
                        .setTransportChannelProvider(getTransportChannelProvider(context))
                        .build();

                try (GrpcPublisherStub publisherStub = GrpcPublisherStub.create(publisherStubSettings)) {
                    final String topicName = context.getProperty(TOPIC_NAME).evaluateAttributeExpressions().getValue();
                    final TestIamPermissionsRequest request = TestIamPermissionsRequest.newBuilder()
                            .addAllPermissions(REQUIRED_PERMISSIONS)
                            .setResource(topicName)
                            .build();
                    final TestIamPermissionsResponse response = publisherStub.testIamPermissionsCallable().call(request);
                    if (response.getPermissionsCount() >= REQUIRED_PERMISSIONS.size()) {
                        results.add(new ConfigVerificationResult.Builder()
                                .verificationStepName("Test IAM Permissions")
                                .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                                .explanation(String.format("Verified Topic [%s] exists and the configured user has the correct permissions.", topicName))
                                .build());
                    } else {
                        results.add(new ConfigVerificationResult.Builder()
                                .verificationStepName("Test IAM Permissions")
                                .outcome(ConfigVerificationResult.Outcome.FAILED)
                                .explanation(String.format("The configured user does not have the correct permissions on Topic [%s].", topicName))
                                .build());
                    }
                }
            } catch (final ApiException e) {
                verificationLogger.error("The configured user appears to have the correct permissions, but the following error was encountered", e);
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Test IAM Permissions")
                        .outcome(ConfigVerificationResult.Outcome.FAILED)
                        .explanation(String.format("The configured user appears to have the correct permissions, but the following error was encountered: " + e.getMessage()))
                        .build());
            } catch (final IOException e) {
                verificationLogger.error("The publisher stub could not be created in order to test the permissions", e);
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Test IAM Permissions")
                        .outcome(ConfigVerificationResult.Outcome.FAILED)
                        .explanation(String.format("The publisher stub could not be created in order to test the permissions: " + e.getMessage()))
                        .build());

            }
        }
        return results;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final StopWatch stopWatch = new StopWatch(true);
        final ContentInputStrategy inputStrategy = ContentInputStrategy.valueOf(context.getProperty(CONTENT_INPUT_STRATEGY).getValue());
        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).asInteger();
        final List<FlowFile> flowFileBatch = session.get(maxBatchSize);
        if (flowFileBatch.isEmpty()) {
            context.yield();
        } else if (storedException.get() != null) {
            getLogger().error("Google Cloud PubSub Publisher was not properly created due to {}", storedException.get());
            context.yield();
        } else if (ContentInputStrategy.FLOWFILE_ORIENTED.equals(inputStrategy)) {
            onTriggerFlowFileStrategy(context, session, stopWatch, flowFileBatch);
        } else if (ContentInputStrategy.RECORD_ORIENTED.equals(inputStrategy)) {
            onTriggerRecordStrategy(context, session, stopWatch, flowFileBatch);
        } else {
            throw new IllegalStateException(inputStrategy.getValue());
        }
    }

    private void onTriggerFlowFileStrategy(
            final ProcessContext context,
            final ProcessSession session,
            final StopWatch stopWatch,
            final List<FlowFile> flowFileBatch) throws ProcessException {
        final long maxMessageSize = context.getProperty(MAX_MESSAGE_SIZE).asDataSize(DataUnit.B).longValue();

        final MessageTracker messageTracker = new MessageTracker();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (final FlowFile flowFile : flowFileBatch) {
            if (flowFile.getSize() > maxMessageSize) {
                final String message = String.format("FlowFile size %d exceeds MAX_MESSAGE_SIZE", flowFile.getSize());
                messageTracker.add(new FlowFileResult(flowFile, Collections.emptyList(), new IllegalArgumentException(message)));
            } else {
                baos.reset();
                session.exportTo(flowFile, baos);
                final ApiFuture<String> future = publishOneMessage(context, flowFile, baos.toByteArray());
                messageTracker.add(new FlowFileResult(flowFile, Collections.singletonList(future)));
            }
        }
        finishBatch(session, flowFileBatch, stopWatch, messageTracker);
    }

    private void onTriggerRecordStrategy(
            final ProcessContext context,
            final ProcessSession session,
            final StopWatch stopWatch,
            final List<FlowFile> flowFileBatch) throws ProcessException {
        try {
            onTriggerRecordStrategyInner(context, session, stopWatch, flowFileBatch);
        } catch (IOException | SchemaNotFoundException | MalformedRecordException e) {
            throw new ProcessException(e);
        }
    }

    private void onTriggerRecordStrategyInner(
            final ProcessContext context,
            final ProcessSession session,
            final StopWatch stopWatch,
            final List<FlowFile> flowFileBatch)
            throws ProcessException, IOException, SchemaNotFoundException, MalformedRecordException {
        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);

        final MessageTracker messageTracker = new MessageTracker();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (final FlowFile flowFile : flowFileBatch) {
            final Map<String, String> attributes = flowFile.getAttributes();
            final RecordReader reader = readerFactory.createRecordReader(
                    attributes, session.read(flowFile), flowFile.getSize(), getLogger());
            final RecordSet recordSet = reader.createRecordSet();
            final RecordSchema schema = writerFactory.getSchema(attributes, recordSet.getSchema());

            final RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, baos, attributes);
            final PushBackRecordSet pushBackRecordSet = new PushBackRecordSet(recordSet);
            final List<ApiFuture<String>> futures = new ArrayList<>();
            while (pushBackRecordSet.isAnotherRecord()) {
                final ApiFuture<String> future = publishOneRecord(context, flowFile, baos, writer, pushBackRecordSet.next());
                futures.add(future);
            }
            messageTracker.add(new FlowFileResult(flowFile, futures));
            getLogger().trace("Parsing of FlowFile (ID:{}) records complete, now {} messages", flowFile.getId(), messageTracker.size());
        }
        finishBatch(session, flowFileBatch, stopWatch, messageTracker);
    }

    private ApiFuture<String> publishOneRecord(
            final ProcessContext context,
            final FlowFile flowFile,
            final ByteArrayOutputStream baos,
            final RecordSetWriter writer,
            final Record record) throws IOException {
        baos.reset();
        writer.write(record);
        writer.flush();
        return publishOneMessage(context, flowFile, baos.toByteArray());
    }

    private ApiFuture<String> publishOneMessage(final ProcessContext context,
                                                final FlowFile flowFile,
                                                final byte[] content) {
        final PubsubMessage message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(content))
                .setPublishTime(Timestamp.newBuilder().build())
                .putAllAttributes(getDynamicAttributesMap(context, flowFile))
                .build();
        return publisher.publish(message);
    }

    private void finishBatch(final ProcessSession session,
                             final List<FlowFile> flowFileBatch,
                             final StopWatch stopWatch,
                             final MessageTracker messageTracker) {
        try {
            getLogger().trace("Submit of batch complete, size {}", messageTracker.size());
            final List<String> messageIdsSuccess = ApiFutures.successfulAsList(messageTracker.getFutures()).get();
            getLogger().trace("Send of batch complete, success size {}", messageIdsSuccess.size());
            messageTracker.reconcile(messageIdsSuccess);
            final String topicName = publisher.getTopicNameString();
            for (final FlowFileResult flowFileResult : messageTracker.getFlowFileResults()) {
                final Map<String, String> attributes = new HashMap<>();
                //attributes.put(MESSAGE_ID_ATTRIBUTE, messageIdFuture.get());  // what to do if using record strategy?
                attributes.put(TOPIC_NAME_ATTRIBUTE, topicName);
                final FlowFile flowFile = session.putAllAttributes(flowFileResult.getFlowFile(), attributes);
                final String transitUri = String.format(TRANSIT_URI_FORMAT_STRING, topicName);
                session.getProvenanceReporter().send(flowFile, transitUri, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                session.transfer(flowFile, flowFileResult.getRelationship());
                if (flowFileResult.getException() != null) {
                    getLogger().error("FlowFile send failure", flowFileResult.getException());
                }
            }
        } catch (final InterruptedException | ExecutionException e) {
            session.rollback();
            final String message = String.format("FlowFile batch processing failed (size=%d)", flowFileBatch.size());
            getLogger().error(message, e);
        }
    }

    @OnStopped
    public void onStopped() {
        shutdownPublisher();
    }

    private void shutdownPublisher() {
        try {
            if (publisher != null) {
                publisher.shutdown();
            }
        } catch (Exception e) {
            getLogger().warn("Failed to gracefully shutdown the Google Cloud PubSub Publisher due to {}", new Object[]{e});
        }
    }

    private ProjectTopicName getTopicName(ProcessContext context) {
        final String topic = context.getProperty(TOPIC_NAME).evaluateAttributeExpressions().getValue();
        final String projectId = context.getProperty(PROJECT_ID).evaluateAttributeExpressions().getValue();

        if (topic.contains("/")) {
            return ProjectTopicName.parse(topic);
        } else {
            return ProjectTopicName.of(projectId, topic);
        }
    }

    private Map<String, String> getDynamicAttributesMap(ProcessContext context, FlowFile flowFile) {
        final Map<String, String> attributes = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                final String value = context.getProperty(entry.getKey()).evaluateAttributeExpressions(flowFile).getValue();
                attributes.put(entry.getKey().getName(), value);
            }
        }

        return attributes;
    }

    private Publisher.Builder getPublisherBuilder(ProcessContext context) {
        final Long batchSizeThreshold = context.getProperty(BATCH_SIZE_THRESHOLD).asLong();
        final long batchBytesThreshold = context.getProperty(BATCH_BYTES_THRESHOLD).asDataSize(DataUnit.B).longValue();
        final Long batchDelayThreshold = context.getProperty(BATCH_DELAY_THRESHOLD).asTimePeriod(TimeUnit.MILLISECONDS);
        final String endpoint = context.getProperty(PUBSUB_ENDPOINT).getValue();

        final Publisher.Builder publisherBuilder = Publisher.newBuilder(getTopicName(context))
                .setCredentialsProvider(FixedCredentialsProvider.create(getGoogleCredentials(context)))
                .setChannelProvider(getTransportChannelProvider(context));

        if (endpoint != null && !endpoint.isEmpty()) {
            publisherBuilder.setEndpoint(endpoint);
        }

        publisherBuilder.setBatchingSettings(BatchingSettings.newBuilder()
                .setElementCountThreshold(batchSizeThreshold)
                .setRequestByteThreshold(batchBytesThreshold)
                .setDelayThreshold(Duration.ofMillis(batchDelayThreshold))
                //.setFlowControlSettings(null)
                .setIsEnabled(true)
                .build());
        return publisherBuilder;
    }
}
