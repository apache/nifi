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
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.DeadlineExceededException;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

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
public class PublishGCPubSub extends AbstractGCPubSubProcessor{

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
    private AtomicReference<Exception> storedException = new AtomicReference<>();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return ImmutableList.of(PROJECT_ID,
                GCP_CREDENTIALS_PROVIDER_SERVICE,
                TOPIC_NAME,
                BATCH_SIZE);
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

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        try {
            publisher = getPublisherBuilder(context).build();
        } catch (IOException e) {
            getLogger().error("Failed to create Google Cloud PubSub Publisher due to {}", new Object[]{e});
            storedException.set(e);
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final int flowFileCount = context.getProperty(BATCH_SIZE).asInteger();
        final List<FlowFile> flowFiles = session.get(flowFileCount);

        if (flowFiles.isEmpty() || publisher == null) {
            if (storedException.get() != null) {
                getLogger().error("Google Cloud PubSub Publisher was not properly created due to {}", new Object[]{storedException.get()});
            }
            context.yield();
            return;
        }

        final long startNanos = System.nanoTime();
        final List<FlowFile> successfulFlowFiles = new ArrayList<>();
        final String topicName = getTopicName(context).toString();

        try {
            for (FlowFile flowFile : flowFiles) {
                try {
                    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    session.exportTo(flowFile, baos);
                    final ByteString flowFileContent = ByteString.copyFromUtf8(baos.toString());

                    PubsubMessage message = PubsubMessage.newBuilder().setData(flowFileContent)
                            .setPublishTime(Timestamp.newBuilder().build())
                            .putAllAttributes(getDynamicAttributesMap(context, flowFile))
                            .build();

                    ApiFuture<String> messageIdFuture = publisher.publish(message);

                    while (messageIdFuture.isDone()) {
                        Thread.sleep(500L);
                    }

                    final String messageId = messageIdFuture.get();
                    final Map<String, String> attributes = new HashMap<>();

                    attributes.put(MESSAGE_ID_ATTRIBUTE, messageId);
                    attributes.put(TOPIC_NAME_ATTRIBUTE, topicName);

                    flowFile = session.putAllAttributes(flowFile, attributes);
                    successfulFlowFiles.add(flowFile);
                } catch (InterruptedException | ExecutionException e) {
                    if (e.getCause() instanceof DeadlineExceededException) {
                        getLogger().error("Failed to publish the message to Google Cloud PubSub topic '{}' due to {} but attempting again may succeed " +
                                        "so routing to retry", new Object[]{topicName, e.getLocalizedMessage()}, e);
                        session.transfer(flowFile, REL_RETRY);
                    } else {
                        getLogger().error("Failed to publish the message to Google Cloud PubSub topic '{}' due to {}",
                                new Object[]{topicName, e});
                        session.transfer(flowFile, REL_FAILURE);
                        context.yield();
                    }
                }
            }
        } finally {
            if (!successfulFlowFiles.isEmpty()) {
                session.transfer(successfulFlowFiles, REL_SUCCESS);
                for (FlowFile flowFile : successfulFlowFiles) {
                    final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                    session.getProvenanceReporter().send(flowFile, topicName, transmissionMillis);
                }
            }
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
        final String projectId = context.getProperty(PROJECT_ID).getValue();

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
        final Long batchSize = context.getProperty(BATCH_SIZE).asLong();

        return Publisher.newBuilder(getTopicName(context))
                .setCredentialsProvider(FixedCredentialsProvider.create(getGoogleCredentials(context)))
                .setBatchingSettings(BatchingSettings.newBuilder()
                .setElementCountThreshold(batchSize)
                .setIsEnabled(true)
                .build());
    }
}
