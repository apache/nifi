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
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;

import java.io.ByteArrayOutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SeeAlso({GetAzureQueueStorage.class})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "azure", "microsoft", "cloud", "storage", "queue", "enqueue" })
@CapabilityDescription("Writes the content of the incoming FlowFiles to the configured Azure Queue Storage.")
public class PutAzureQueueStorage extends AbstractAzureQueueStorage {

    public static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
            .name("time-to-live")
            .displayName("TTL")
            .description("Maximum time to allow the message to be in the queue. If left empty, the default value of 7 days will be used.")
            .required(false)
            .defaultValue("7 days")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor VISIBILITY_DELAY = new PropertyDescriptor.Builder()
            .name("visibility-delay")
            .displayName("Visibility Delay")
            .description("The length of time during which the message will be invisible, starting when it is added to the queue. " +
                         "This value must be greater than or equal to 0 and less than the TTL value.")
            .required(false)
            .defaultValue("0 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties =  Collections.unmodifiableList(Arrays.asList(
            AzureStorageUtils.ACCOUNT_NAME, AzureStorageUtils.ACCOUNT_KEY, AzureStorageUtils.PROP_SAS_TOKEN, TTL,
            QUEUE, VISIBILITY_DELAY, AzureStorageUtils.PROXY_CONFIGURATION_SERVICE));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(flowFile, baos);
        final String flowFileContent = baos.toString();

        CloudQueueMessage message = new CloudQueueMessage(flowFileContent);
        CloudQueueClient cloudQueueClient;
        CloudQueue cloudQueue;

        final int ttl = context.getProperty(TTL).asTimePeriod(TimeUnit.SECONDS).intValue();
        final int delay = context.getProperty(VISIBILITY_DELAY).asTimePeriod(TimeUnit.SECONDS).intValue();
        final String queue = context.getProperty(QUEUE).evaluateAttributeExpressions(flowFile).getValue().toLowerCase();

        try {
            cloudQueueClient = createCloudQueueClient(context, flowFile);
            cloudQueue = cloudQueueClient.getQueueReference(queue);

            final OperationContext operationContext = new OperationContext();
            AzureStorageUtils.setProxy(operationContext, context);

            cloudQueue.addMessage(message, ttl, delay, null, operationContext);
        } catch (URISyntaxException | StorageException e) {
            getLogger().error("Failed to write the message to Azure Queue Storage due to {}", new Object[]{e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
        final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        session.getProvenanceReporter().send(flowFile, cloudQueue.getUri().toString(), transmissionMillis);
    }

    @Override
    public Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));

        final boolean ttlSet = validationContext.getProperty(TTL).isSet();
        final boolean delaySet = validationContext.getProperty(VISIBILITY_DELAY).isSet();

        final int ttl = validationContext.getProperty(TTL).asTimePeriod(TimeUnit.SECONDS).intValue();

        if (ttlSet) {
            final int SEVEN_DAYS_TIMEPERIOD_IN_SECS = 604800; // i.e. 7 * 24 * 60 * 60

            if (ttl > SEVEN_DAYS_TIMEPERIOD_IN_SECS) {
                problems.add(new ValidationResult.Builder()
                                                 .subject(TTL.getDisplayName())
                                                 .valid(false)
                                                 .explanation(TTL.getDisplayName() + " exceeds the allowed limit of 7 days. Set a value less than 7 days")
                                                 .build());
            }
        }

        if (delaySet) {
            int delay = validationContext.getProperty(VISIBILITY_DELAY).asTimePeriod(TimeUnit.SECONDS).intValue();

            if (delay > ttl || delay < 0) {
                problems.add(new ValidationResult.Builder()
                                                 .subject(VISIBILITY_DELAY.getDisplayName())
                                                 .valid(false)
                                                 .explanation(VISIBILITY_DELAY.getDisplayName() + " should be greater than or equal to 0 and less than " + TTL.getDisplayName())
                                                 .build());
            }
        }

        AzureStorageUtils.validateProxySpec(validationContext, problems);

        return problems;
    }
}
