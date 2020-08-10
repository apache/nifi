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
package org.apache.nifi.processors.xmpp;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import rocks.xmpp.core.stanza.MessageEvent;

import java.util.AbstractQueue;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

@Tags({"get", "xmpp", "fetch", "ingress", "ingest", "source", "input", "retrieve", "listen", "read"})
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Fetches XMPP messages and create FlowFiles from them")
@SeeAlso({PutXMPP.class})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GetXMPP extends AbstractXMPPProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received are routed to success")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private final AbstractQueue<MessageEvent> messageEvents = new ConcurrentLinkedQueue<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = Collections.unmodifiableList(getBasePropertyDescriptors());
        this.relationships = createRelationships(SUCCESS);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        registerListeners();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (messageEvents.isEmpty()) {
            context.yield();
            return;
        }
        handleMessageEventsQueue(session);
    }

    private void registerListeners() {
        if (chatRoom != null) {
            chatRoom.addInboundMessageListener(messageEvents::add);
        } else {
            xmppClient.addInboundMessageListener(messageEvents::add);
        }
    }

    private void handleMessageEventsQueue(ProcessSession session) {
        MessageEvent messageEvent;
        while ((messageEvent = messageEvents.poll()) != null) {
            sendMessageEventAsFlowFile(session, messageEvent);
        }
    }

    private void sendMessageEventAsFlowFile(ProcessSession session, MessageEvent messageEvent) {
        final FlowFile flowFile = writeToFlowFile(session, messageEvent.getMessage().getBody());
        session.transfer(flowFile, SUCCESS);
        session.commit();
    }

    private FlowFile writeToFlowFile(ProcessSession session, String contents) {
        return session.write(session.create(), outputStream -> {
            outputStream.write(contents.getBytes());
        });
    }
}
