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
package org.apache.nifi.processors.azure.eventhub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.IllegalConnectionStringFormatException;
import com.microsoft.azure.eventhubs.impl.EventHubClientImpl;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

@SupportsBatching
@Tags({"microsoft", "azure", "cloud", "eventhub", "events", "streams", "streaming"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sends the contents of a FlowFile to Windows Azure Event Hubs. Note: the content of the FlowFile will be buffered into memory before being sent, "
        + "so care should be taken to avoid sending FlowFiles to this Processor that exceed the amount of Java Heap Space available. "
        + "Also please be aware that this processor creates a thread pool of 4 threads for Event Hub Client. They will be extra threads other than the concurrent tasks scheduled for this processor.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
public class PutAzureEventHub extends AbstractProcessor {
    static final PropertyDescriptor EVENT_HUB_NAME = new PropertyDescriptor.Builder()
            .name("Event Hub Name")
            .description("The name of the event hub to send to")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .build();
    static final PropertyDescriptor NAMESPACE = new PropertyDescriptor.Builder()
            .name("Event Hub Namespace")
            .description("The namespace that the event hub is assigned to. This is generally equal to <Event Hubs Name>-ns")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();
    static final PropertyDescriptor ACCESS_POLICY = new PropertyDescriptor.Builder()
            .name("Shared Access Policy Name")
            .description("The name of the shared access policy. This policy must have Send claims.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();
    static final PropertyDescriptor POLICY_PRIMARY_KEY = new PropertyDescriptor.Builder()
            .name("Shared Access Policy Primary Key")
            .description("The primary key of the shared access policy")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .sensitive(true)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully sent to the event hubs will be transferred to this Relationship.")
            .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that could not be sent to the event hub will be transferred to this Relationship.")
            .build();

    private volatile BlockingQueue<EventHubClient> senderQueue = new LinkedBlockingQueue<>();

    private final static List<PropertyDescriptor> propertyDescriptors;
    private final static Set<Relationship> relationships;

    /*
    * Will ensure that the list of property descriptors is build only once.
    * Will also create a Set of relationships
    */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(EVENT_HUB_NAME);
        _propertyDescriptors.add(NAMESPACE);
        _propertyDescriptors.add(ACCESS_POLICY);
        _propertyDescriptors.add(POLICY_PRIMARY_KEY);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    private ScheduledExecutorService executor;

    @OnScheduled
    public final void setupClient(final ProcessContext context) throws ProcessException{
    }

    @OnStopped
    public void tearDown() {
        EventHubClient sender;
        while ((sender = senderQueue.poll()) != null) {
            sender.close();
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if(senderQueue.size() == 0){
            final int numThreads = context.getMaxConcurrentTasks();
            senderQueue = new LinkedBlockingQueue<>(numThreads);
            executor = Executors.newScheduledThreadPool(4);
            final String policyName = context.getProperty(ACCESS_POLICY).getValue();
            final String policyKey = context.getProperty(POLICY_PRIMARY_KEY).getValue();
            final String namespace = context.getProperty(NAMESPACE).getValue();
            final String eventHubName = context.getProperty(EVENT_HUB_NAME).getValue();
            for (int i = 0; i < numThreads; i++) {
                final EventHubClient client = createEventHubClient(namespace, eventHubName, policyName, policyKey, executor);
                if(null != client) {
                    senderQueue.offer(client);
                }
            }
        }

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
            final byte[] buffer = new byte[(int) flowFile.getSize()];
            session.read(flowFile, in -> StreamUtils.fillBuffer(in, buffer));

            try {
                sendMessage(buffer);
            } catch (final ProcessException processException) {
                getLogger().error("Failed to send {} to EventHub due to {}; routing to failure", new Object[]{flowFile, processException}, processException);
                session.transfer(session.penalize(flowFile), REL_FAILURE);
                return;
            }

            final String namespace = context.getProperty(NAMESPACE).getValue();
            final String eventHubName = context.getProperty(EVENT_HUB_NAME).getValue();
            session.getProvenanceReporter().send(flowFile, "amqps://" + namespace + ".servicebus.windows.net" + "/" + eventHubName, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);

    }

    protected EventHubClient createEventHubClient(
        final String namespace,
        final String eventHubName,
        final String policyName,
        final String policyKey,
        final ScheduledExecutorService executor)
        throws ProcessException{

        try {
            EventHubClientImpl.USER_AGENT = "ApacheNiFi-azureeventhub/2.3.2";
            return EventHubClient.createSync(getConnectionString(namespace, eventHubName, policyName, policyKey), executor);
        } catch (IOException | EventHubException | IllegalConnectionStringFormatException e) {
            getLogger().error("Failed to create EventHubClient due to {}", e);
            throw new ProcessException(e);
        }
    }
    protected String getConnectionString(final String namespace, final String eventHubName, final String policyName, final String policyKey){
        return new ConnectionStringBuilder().setNamespaceName(namespace).setEventHubName(eventHubName).setSasKeyName(policyName).setSasKey(policyKey).toString();
    }
    protected void sendMessage(final byte[] buffer) throws ProcessException {

        final EventHubClient sender = senderQueue.poll();
        if(null != sender) {
            try {
                sender.sendSync(EventData.create(buffer));
            } catch (final EventHubException sbe) {
                throw new ProcessException("Caught exception trying to send message to eventbus", sbe);
            } finally {
                senderQueue.offer(sender);
            }
        }else{
            throw new ProcessException("No EventHubClients are configured for sending");
        }
    }
}
