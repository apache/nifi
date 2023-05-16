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

import com.azure.core.util.Context;
import com.azure.storage.queue.QueueClient;
import com.azure.storage.queue.models.QueueStorageException;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SeeAlso({GetAzureQueueStorage_v12.class})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({ "azure", "microsoft", "cloud", "storage", "queue", "enqueue" })
@CapabilityDescription("Writes the content of the incoming FlowFiles to the configured Azure Queue Storage.")
public class PutAzureQueueStorage_v12 extends AbstractAzureQueueStorage_v12 {
    public static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
            .name("time-to-live")
            .displayName("TTL")
            .description("Maximum time to allow the message to be in the queue. If left empty, the default value of 7 days will be used.")
            .required(false)
            .defaultValue("7 days")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor VISIBILITY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("visibility-timeout")
            .displayName("Visibility Timeout")
            .description("The length of time during which the message will be invisible after it is read. " +
                    "If the processing unit fails to delete the message after it is read, then the message will reappear in the queue. " +
                    "If left empty, the default value of 30 secs will be used.")
            .required(false)
            .defaultValue("30 secs")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP, ProxySpec.SOCKS};
    private static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(
                    QUEUE,
                    STORAGE_CREDENTIALS_SERVICE,
                    TTL,
                    VISIBILITY_TIMEOUT,
                    REQUEST_TIMEOUT,
                    ProxyConfiguration.createProxyConfigPropertyDescriptor(false, PROXY_SPECS)
            )
    );

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final long startNanos = System.nanoTime();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        session.exportTo(flowFile, baos);
        final String flowFileContent = baos.toString();

        final int visibilityTimeoutInSecs = context.getProperty(VISIBILITY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final int ttl = context.getProperty(TTL).asTimePeriod(TimeUnit.SECONDS).intValue();
        final int requestTimeoutInSecs = context.getProperty(REQUEST_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();

        final QueueClient queueClient;
        queueClient = createQueueClient(context, flowFile);
        try {
            queueClient.sendMessageWithResponse(
                    flowFileContent,
                    Duration.ofSeconds(visibilityTimeoutInSecs),
                    Duration.ofSeconds(ttl),
                    Duration.ofSeconds(requestTimeoutInSecs),
                    Context.NONE
            );
        } catch (final QueueStorageException e) {
            getLogger().error("Failed to write the message to Azure Queue Storage due to {}", new Object[]{e});
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
        final long transmissionMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
        session.getProvenanceReporter().send(flowFile, queueClient.getQueueUrl().toString(), transmissionMillis);
    }
}
