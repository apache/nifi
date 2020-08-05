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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import rocks.xmpp.addr.Jid;
import rocks.xmpp.core.XmppException;
import rocks.xmpp.core.net.ChannelEncryption;
import rocks.xmpp.core.net.client.SocketConnectionConfiguration;
import rocks.xmpp.core.session.XmppClient;
import rocks.xmpp.core.stanza.model.Message;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Tags({"put", "xmpp", "notify", "send", "publish"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sends a direct message using XMPP")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class PutXMPP extends AbstractProcessor {

    public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor
            .Builder().name("hostname")
            .displayName("Hostname")
            .description("The IP address or hostname of the XMPP server")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("port")
            .displayName("Port")
            .description("The port on the XMPP server")
            .required(true)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    public static final PropertyDescriptor XMPP_DOMAIN = new PropertyDescriptor
            .Builder().name("xmpp-domain")
            .displayName("XMPP Domain")
            .description("The XMPP domain")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SOURCE_USER = new PropertyDescriptor
            .Builder().name("source-user")
            .displayName("Source user")
            .description("The name of the user to send the XMPP messages as")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor
            .Builder().name("password")
            .displayName("Password")
            .description("Password for the source user account")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor TARGET_USER = new PropertyDescriptor
            .Builder().name("target-user")
            .displayName("Target User")
            .description("The name of the user to send the XMPP message to")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RESOURCE = new PropertyDescriptor
            .Builder().name("resource")
            .displayName("Resource")
            .description("The XMPP resource to send messages to")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MY_PROPERTY = new PropertyDescriptor
            .Builder().name("MY_PROPERTY")
            .displayName("My property")
            .description("Example Property")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to success after being successfully sent as XMPP messages")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to failure if unable to be sent as XMPP messages")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private XmppClient xmppClient;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HOSTNAME);
        descriptors.add(PORT);
        descriptors.add(XMPP_DOMAIN);
        descriptors.add(SOURCE_USER);
        descriptors.add(PASSWORD);
        descriptors.add(TARGET_USER);
        descriptors.add(RESOURCE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final SocketConnectionConfiguration socketConfiguration = SocketConnectionConfiguration.builder()
            .hostname(context.getProperty(HOSTNAME).getValue())
            .port(context.getProperty(PORT).asInteger())
            .channelEncryption(ChannelEncryption.DISABLED)
            .build();
        xmppClient = XmppClient.create(context.getProperty(XMPP_DOMAIN).getValue(), socketConfiguration);
        try {
            xmppClient.connect();
        } catch (XmppException e) {
            getLogger().error("Failed to connect to the XMPP server", e);
            throw new RuntimeException(e);
        }
        try {
            xmppClient.login(
                context.getProperty(SOURCE_USER).getValue(),
                context.getProperty(PASSWORD).getValue(),
                context.getProperty(RESOURCE).getValue());
        } catch (XmppException e) {
            getLogger().error("Failed to login to the XMPP server", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        final String targetUser = context.getProperty(TARGET_USER).getValue();
        final String xmppDomain = context.getProperty(XMPP_DOMAIN).getValue();
        final Jid to = Jid.of(targetUser + "@" + xmppDomain);
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        session.exportTo(flowFile, bytes);
        final String body = bytes.toString();
        try {
            xmppClient.send(new Message(to, Message.Type.CHAT, body)).get();
            session.transfer(flowFile, SUCCESS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            session.transfer(flowFile, SUCCESS);
        } catch (ExecutionException e) {
            getLogger().error("Failed to send XMPP message", e);
            session.transfer(flowFile, FAILURE);
        }
    }
}
