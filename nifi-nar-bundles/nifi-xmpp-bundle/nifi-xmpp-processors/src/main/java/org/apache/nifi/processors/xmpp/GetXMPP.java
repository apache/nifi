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
import rocks.xmpp.addr.Jid;
import rocks.xmpp.core.stanza.MessageEvent;
import rocks.xmpp.core.stanza.model.Message;

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
@WritesAttributes({
        @WritesAttribute(attribute = GetXMPP.TYPE, description = "The type of the message (e.g. CHAT or GROUPCHAT)"),
        @WritesAttribute(attribute = GetXMPP.SUBJECT, description = "The subject of the message"),
        @WritesAttribute(attribute = GetXMPP.THREAD, description = "The thread of the message"),
        @WritesAttribute(attribute = GetXMPP.PARENT_THREAD, description = "The parent thread of the message"),
        @WritesAttribute(attribute = GetXMPP.ID, description = "The ID of the message"),
        @WritesAttribute(attribute = GetXMPP.FROM, description = "The full JID of the sender (e.g. 'local@domain/resource')"),
        @WritesAttribute(attribute = GetXMPP.FROM_BARE_JID, description = "The bare JID of the sender (e.g. 'local@domain')"),
        @WritesAttribute(attribute = GetXMPP.FROM_LOCAL, description = "The local part of the sender's JID"),
        @WritesAttribute(attribute = GetXMPP.FROM_DOMAIN, description = "The domain part of the sender's JID"),
        @WritesAttribute(attribute = GetXMPP.FROM_RESOURCE, description = "The resource part of the sender's JID"),
        @WritesAttribute(attribute = GetXMPP.TO, description = "The full JID of the recipient (e.g. 'local@domain/resource')"),
        @WritesAttribute(attribute = GetXMPP.TO_BARE_JID, description = "The bare JID of the recipient (e.g. 'local@domain')"),
        @WritesAttribute(attribute = GetXMPP.TO_LOCAL, description = "The local part of the recipient's JID"),
        @WritesAttribute(attribute = GetXMPP.TO_DOMAIN, description = "The domain part of the recipient's JID"),
        @WritesAttribute(attribute = GetXMPP.TO_RESOURCE, description = "The resource part of the recipient's JID"),
        @WritesAttribute(attribute = GetXMPP.LANGUAGE, description = "The language of the message")
})
public class GetXMPP extends AbstractXMPPProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received are routed to success")
            .build();

    public static final String TYPE = "xmpp.message.type";
    public static final String SUBJECT = "xmpp.message.subject";
    public static final String THREAD = "xmpp.message.thread";
    public static final String PARENT_THREAD = "xmpp.message.parentThread";
    public static final String ID = "xmpp.message.id";
    public static final String FROM = "xmpp.message.from";
    public static final String FROM_BARE_JID = "xmpp.message.from.bareJID";
    public static final String FROM_LOCAL = "xmpp.message.from.local";
    public static final String FROM_DOMAIN = "xmpp.message.from.domain";
    public static final String FROM_RESOURCE = "xmpp.message.from.resource";
    public static final String TO = "xmpp.message.to";
    public static final String TO_BARE_JID = "xmpp.message.to.bareJID";
    public static final String TO_LOCAL = "xmpp.message.to.local";
    public static final String TO_DOMAIN = "xmpp.message.to.domain";
    public static final String TO_RESOURCE = "xmpp.message.to.resource";
    public static final String LANGUAGE = "xmpp.message.language";

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
        final Message message = messageEvent.getMessage();
        final FlowFile flowFile = writeToFlowFile(session, message.getBody());
        writeFlowFileAttributes(session, flowFile, message);
        session.transfer(flowFile, SUCCESS);
        session.commit();
    }

    private FlowFile writeToFlowFile(ProcessSession session, String contents) {
        return session.write(session.create(), outputStream -> {
            outputStream.write(contents.getBytes());
        });
    }

    private void writeFlowFileAttributes(ProcessSession session, FlowFile flowFile, Message message) {
        writeTypeAttributeIfAvailable(session, flowFile, message);
        writeSubjectAttributeIfAvailable(session, flowFile, message);
        writeThreadAttributeIfAvailable(session, flowFile, message);
        writeParentThreadAttributeIfAvailable(session, flowFile, message);
        writeIdAttributeIfAvailable(session, flowFile, message);
        writeToAttributesIfAvailable(session, flowFile, message);
        writeFromAttributesIfAvailable(session, flowFile, message);
        writeLanguageAttributeIfAvailable(session, flowFile, message);
    }

    private void writeTypeAttributeIfAvailable(ProcessSession session, FlowFile flowFile, Message message) {
        writeObjectAsStringIfAvailable(session, flowFile, GetXMPP.TYPE, message.getType());
    }

    private void writeSubjectAttributeIfAvailable(ProcessSession session, FlowFile flowFile, Message message) {
        writeStringAttributeIfAvailable(session, flowFile, GetXMPP.SUBJECT, message.getSubject());
    }

    private void writeThreadAttributeIfAvailable(ProcessSession session, FlowFile flowFile, Message message) {
        writeStringAttributeIfAvailable(session, flowFile, GetXMPP.THREAD, message.getThread());
    }

    private void writeParentThreadAttributeIfAvailable(ProcessSession session, FlowFile flowFile, Message message) {
        writeStringAttributeIfAvailable(session, flowFile, GetXMPP.PARENT_THREAD, message.getParentThread());
    }

    private void writeIdAttributeIfAvailable(ProcessSession session, FlowFile flowFile, Message message) {
        writeStringAttributeIfAvailable(session, flowFile, GetXMPP.ID, message.getId());
    }

    private void writeToAttributesIfAvailable(ProcessSession session, FlowFile flowFile, Message message) {
        writeJidAttributesIfAvailable(
                session,
                flowFile,
                message.getTo(),
                GetXMPP.TO,
                GetXMPP.TO_BARE_JID,
                GetXMPP.TO_LOCAL,
                GetXMPP.TO_DOMAIN,
                GetXMPP.TO_RESOURCE);
    }

    private void writeFromAttributesIfAvailable(ProcessSession session, FlowFile flowFile, Message message) {
        writeJidAttributesIfAvailable(
                session,
                flowFile,
                message.getFrom(),
                GetXMPP.FROM,
                GetXMPP.FROM_BARE_JID,
                GetXMPP.FROM_LOCAL,
                GetXMPP.FROM_DOMAIN,
                GetXMPP.FROM_RESOURCE);
    }

    private void writeLanguageAttributeIfAvailable(ProcessSession session, FlowFile flowFile, Message message) {
        writeObjectAsStringIfAvailable(session, flowFile, GetXMPP.LANGUAGE, message.getLanguage());
    }

    private void writeObjectAsStringIfAvailable(ProcessSession session, FlowFile flowFile, String attributeName, Object attributeValue) {
        if (attributeValue != null) {
            session.putAttribute(flowFile, attributeName, attributeValue.toString());
        }
    }

    private void writeStringAttributeIfAvailable(ProcessSession session, FlowFile flowFile, String attributeName, String attributeValue) {
        if (attributeValue != null) {
            session.putAttribute(flowFile, attributeName, attributeValue);
        }
    }

    private void writeJidAttributesIfAvailable(
            ProcessSession session,
            FlowFile flowFile,
            Jid jid,
            String fullJidAttributeName,
            String bareJidAttributeName,
            String localAttributeName,
            String domainAttributeName,
            String resourceAttributeName
    ) {
        if (jid != null) {
            session.putAttribute(flowFile, fullJidAttributeName, jid.toString());
            session.putAttribute(flowFile, bareJidAttributeName, jid.asBareJid().toString());
            session.putAttribute(flowFile, localAttributeName, jid.getLocal());
            session.putAttribute(flowFile, domainAttributeName, jid.getDomain());
            session.putAttribute(flowFile, resourceAttributeName, jid.getResource());
        }
    }
}
