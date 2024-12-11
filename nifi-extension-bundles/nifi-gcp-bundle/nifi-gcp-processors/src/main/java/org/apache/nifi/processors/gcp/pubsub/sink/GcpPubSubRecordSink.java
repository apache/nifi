package org.apache.nifi.processors.gcp.pubsub.sink;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.SupportsDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.sink.RecordSinkService;
import org.apache.nifi.record.write.RecordSetWriter;
import org.apache.nifi.record.write.RecordSetWriterFactory;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.StringUtils;

import org.apache.nifi.processors.gcp.credentials.service.GCPCredentialsService;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.processor.exception.ProcessException;

/**
 * A RecordSinkService implementation that publishes record sets to a GCP Pub/Sub topic.
 * 
 * This controller service can be used by processors like PutRecord to write NiFi records 
 * to Google Pub/Sub.
 */
@Tags({"gcp", "google", "pubsub", "records", "sink"})
@CapabilityDescription("Publishes a record set to a configured GCP Pub/Sub topic. This RecordSinkService " +
    "allows a PutRecord processor (or similar) to send serialized records as a single message to a Pub/Sub topic.")
@SupportsDynamicProperties
public class GcpPubSubRecordSink extends AbstractControllerService implements RecordSinkService {

    public static final PropertyDescriptor PROJECT_ID = new PropertyDescriptor.Builder()
            .name("gcp-project-id")
            .displayName("GCP Project ID")
            .description("The ID of the GCP project containing the Pub/Sub topic.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TOPIC_NAME = new PropertyDescriptor.Builder()
            .name("gcp-pubsub-topic")
            .displayName("Pub/Sub Topic Name")
            .description("Name of the Pub/Sub topic to publish messages to.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor GCP_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("gcp-credentials-service")
            .displayName("GCP Credentials Provider Service")
            .description("Controller service that provides GCP credentials for accessing Pub/Sub.")
            .identifiesControllerService(GCPCredentialsService.class)
            .required(true)
            .build();

    private volatile Publisher publisher;

    private volatile String configuredProjectId;
    private volatile String configuredTopicId;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(PROJECT_ID);
        descriptors.add(TOPIC_NAME);
        descriptors.add(GCP_CREDENTIALS_SERVICE);
        return descriptors;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        List<ValidationResult> results = new ArrayList<>();
        final String projectId = context.getProperty(PROJECT_ID).getValue();
        final String topicName = context.getProperty(TOPIC_NAME).getValue();

        if (StringUtils.isEmpty(projectId)) {
            results.add(new ValidationResult.Builder()
                    .subject(PROJECT_ID.getDisplayName())
                    .valid(false)
                    .explanation("Project ID must not be empty.")
                    .build());
        }

        if (StringUtils.isEmpty(topicName)) {
            results.add(new ValidationResult.Builder()
                    .subject(TOPIC_NAME.getDisplayName())
                    .valid(false)
                    .explanation("Topic name must not be empty.")
                    .build());
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.configuredProjectId = context.getProperty(PROJECT_ID).getValue();
        this.configuredTopicId = context.getProperty(TOPIC_NAME).getValue();

        final GCPCredentialsService credentialsService = context.getProperty(GCP_CREDENTIALS_SERVICE)
                .asControllerService(GCPCredentialsService.class);

        try {
            TopicName topicName = TopicName.of(configuredProjectId, configuredTopicId);
            Publisher.Builder publisherBuilder = Publisher.newBuilder(topicName);
            credentialsService.applyCredentials(publisherBuilder);
            this.publisher = publisherBuilder.build();
            getLogger().info("Initialized Pub/Sub Publisher for topic {} in project {}",
                    new Object[]{configuredTopicId, configuredProjectId});
        } catch (IOException e) {
            throw new ProcessException("Failed to create Pub/Sub Publisher", e);
        }
    }

    @OnDisabled
    public void onDisabled() {
        if (publisher != null) {
            try {
                publisher.shutdown();
                publisher.awaitTermination(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                getLogger().warn("Failed to gracefully shutdown Pub/Sub publisher", e);
            } finally {
                publisher = null;
            }
        }
    }

    @Override
    public String getIdentifier() {
        return super.getIdentifier();
    }

    @Override
    public String getDisplayName() {
        return "GCP Pub/Sub Record Sink";
    }

    @Override
    public String sendData(final RecordSet recordSet, final RecordSetWriterFactory writerFactory, final Map<String, String> attributes) throws IOException {
        if (publisher == null) {
            throw new ProcessException("Publisher is not initialized.");
        }

        if (recordSet == null || recordSet.isEmpty()) {
            getLogger().debug("RecordSet is empty, nothing to send.");
            return "No records sent.";
        }

        // Convert entire RecordSet to a single serialized blob
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RecordSetWriter writer = null;
        int recordCount = 0;

        try {
            writer = writerFactory.createWriter(getLogger(), recordSet.getSchema(), baos, attributes);
            writer.beginRecordSet();

            Record record;
            while ((record = recordSet.next()) != null) {
                writer.write(record);
                recordCount++;
            }

            writer.finishRecordSet();
        } catch (SchemaNotFoundException e) {
            throw new IOException("Failed to write record set: Schema not found.", e);
        } catch (Exception e) {
            throw new IOException("Failed to write records to output stream.", e);
        } finally {
            if (writer != null) {
                writer.close();
            }
        }

        byte[] serializedRecords = baos.toByteArray();

        PubsubMessage message = PubsubMessage.newBuilder()
                .setData(ByteString.copyFrom(serializedRecords))
                .build();

        // Publish the message and wait for the result
        ApiFuture<String> futureMessageId = publisher.publish(message);
        String messageId;
        try {
            messageId = futureMessageId.get();
        } catch (Exception e) {
            throw new IOException("Failed to publish message to Pub/Sub: " + e.getMessage(), e);
        }

        return "Published " + recordCount + " records to Pub/Sub topic " + configuredTopicId + " with message ID: " + messageId;
    }

    @Override
    public String sendData(RecordSet recordSet, RecordSetWriterFactory writerFactory) throws IOException {
        return sendData(recordSet, writerFactory, Collections.emptyMap());
    }

    @Override
    public String sendData(byte[] data, Map<String, String> attributes) throws IOException {
        // For this RecordSinkService, we rely on RecordSets rather than raw byte arrays.
        // If needed, implement a way to directly send raw byte arrays as a message.
        throw new UnsupportedOperationException("Sending raw byte[] data is not supported by this RecordSinkService.");
    }

    @Override
    public boolean isScalable() {
        // Generally, a controller service is shared and doesn't scale like a processor.
        return false;
    }

    @Override
    public void shutdown() {
        // This is not mandatory but can be used if NiFi's lifecycle calls it.
        if (publisher != null) {
            try {
                publisher.shutdown();
                publisher.awaitTermination(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                getLogger().warn("Failed to gracefully shutdown Pub/Sub publisher on shutdown.", e);
            } finally {
                publisher = null;
            }
        }
    }
}
