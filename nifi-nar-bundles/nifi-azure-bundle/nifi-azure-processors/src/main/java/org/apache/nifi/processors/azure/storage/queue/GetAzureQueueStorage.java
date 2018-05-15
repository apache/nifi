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
package org.apache.nifi.processors.azure.storage.queue;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.queue.CloudQueue;
import com.microsoft.azure.storage.queue.CloudQueueClient;
import com.microsoft.azure.storage.queue.CloudQueueMessage;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Collections;
import java.util.Arrays;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

@SeeAlso({PutAzureQueueStorage.class})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"azure", "queue", "microsoft", "storage", "dequeue", "cloud"})
@CapabilityDescription("Retrieves the messages from an Azure Queue Storage. The retrieved messages will be deleted from the queue by default. If the requirement is " +
        "to consume messages without deleting them, set 'Auto Delete Messages' to 'false'. Note: There might be chances of receiving duplicates in situations like " +
        "when a message is received but was unable to be deleted from the queue due to some unexpected situations.")
@WritesAttributes({
        @WritesAttribute(attribute = "azure.queue.uri", description = "The absolute URI of the configured Azure Queue Storage"),
        @WritesAttribute(attribute = "azure.queue.insertionTime", description = "The time when the message was inserted into the queue storage"),
        @WritesAttribute(attribute = "azure.queue.expirationTime", description = "The time when the message will expire from the queue storage"),
        @WritesAttribute(attribute = "azure.queue.messageId", description = "The ID of the retrieved message"),
        @WritesAttribute(attribute = "azure.queue.popReceipt", description = "The pop receipt of the retrieved message"),
})
public class GetAzureQueueStorage extends AbstractAzureQueueStorage {

    public static final PropertyDescriptor AUTO_DELETE = new PropertyDescriptor.Builder()
            .name("auto-delete-messages")
            .displayName("Auto Delete Messages")
            .description("Specifies whether the received message is to be automatically deleted from the queue.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("batch-size")
            .displayName("Batch Size")
            .description("The number of messages to be retrieved from the queue.")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, 32, true))
            .defaultValue("32")
            .build();

    public static final PropertyDescriptor VISIBILITY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("visibility-timeout")
            .displayName("Visibility Timeout")
            .description("The duration during which the retrieved message should be invisible to other consumers.")
            .required(true)
            .defaultValue("30 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(
            AzureStorageUtils.ACCOUNT_NAME, AzureStorageUtils.ACCOUNT_KEY, AzureStorageUtils.PROP_SAS_TOKEN, QUEUE, AUTO_DELETE,
            BATCH_SIZE, VISIBILITY_TIMEOUT, AzureStorageUtils.PROXY_CONFIGURATION_SERVICE));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.singleton(REL_SUCCESS);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {

        final int visibilityTimeoutInSecs = context.getProperty(VISIBILITY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final boolean autoDelete = context.getProperty(AUTO_DELETE).asBoolean();
        final String queue = context.getProperty(QUEUE).evaluateAttributeExpressions().getValue().toLowerCase();

        final Iterable<CloudQueueMessage> retrievedMessagesIterable;

        CloudQueueClient cloudQueueClient;
        CloudQueue cloudQueue;

        try {
            cloudQueueClient = createCloudQueueClient(context, null);
            cloudQueue = cloudQueueClient.getQueueReference(queue);

            final OperationContext operationContext = new OperationContext();
            AzureStorageUtils.setProxy(operationContext, context);

            retrievedMessagesIterable = cloudQueue.retrieveMessages(batchSize, visibilityTimeoutInSecs, null, operationContext);
        } catch (URISyntaxException | StorageException e) {
            getLogger().error("Failed to retrieve messages from the provided Azure Storage Queue due to {}", new Object[] {e});
            context.yield();
            return;
        }

        final List<CloudQueueMessage> cloudQueueMessages = toList(retrievedMessagesIterable);

        for (final CloudQueueMessage message : cloudQueueMessages) {
            FlowFile flowFile = session.create();

            final Map<String, String> attributes = new HashMap<>();

            attributes.put("azure.queue.uri", cloudQueue.getUri().toString());
            attributes.put("azure.queue.insertionTime", message.getInsertionTime().toString());
            attributes.put("azure.queue.expirationTime", message.getExpirationTime().toString());
            attributes.put("azure.queue.messageId", message.getMessageId());
            attributes.put("azure.queue.popReceipt", message.getPopReceipt());

            flowFile = session.putAllAttributes(flowFile, attributes);
            flowFile = session.write(flowFile, out -> {
                try {
                    out.write(message.getMessageContentAsByte());
                } catch (StorageException e) {
                    getLogger().error("Failed to write the retrieved queue message to FlowFile content due to {}", new Object[] {e});
                    context.yield();
                }
            });

            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().receive(flowFile, cloudQueue.getStorageUri().toString());
        }

        if(autoDelete) {
            session.commit();

            for (final CloudQueueMessage message : cloudQueueMessages) {
                try {
                    cloudQueue.deleteMessage(message);
                } catch (StorageException e) {
                    getLogger().error("Failed to delete the retrieved message with the id {} from the queue due to {}",
                            new Object[] {message.getMessageId(), e});
                }
            }
        }

    }

    @Override
    public Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));
        final int visibilityTimeout = validationContext.getProperty(VISIBILITY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();

        if (visibilityTimeout <= 0) {
            problems.add(new ValidationResult.Builder()
                                             .valid(false)
                                             .subject(VISIBILITY_TIMEOUT.getDisplayName())
                                             .explanation(VISIBILITY_TIMEOUT.getDisplayName() + " should be greater than 0 secs")
                                             .build());
        }

        AzureStorageUtils.validateProxySpec(validationContext, problems);

        return problems;
    }

    private List<CloudQueueMessage> toList(Iterable<CloudQueueMessage> iterable) {
        if (iterable instanceof List) {
            return (List<CloudQueueMessage>) iterable;
        }

        final ArrayList<CloudQueueMessage> list = new ArrayList<>();
        if (iterable != null) {
            for(CloudQueueMessage message : iterable) {
                list.add(message);
            }
        }

        return list;
    }

}
