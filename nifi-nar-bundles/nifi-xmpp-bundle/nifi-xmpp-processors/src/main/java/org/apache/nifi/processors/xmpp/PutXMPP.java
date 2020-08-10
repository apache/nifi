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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import rocks.xmpp.addr.Jid;
import rocks.xmpp.core.stanza.model.Message;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Tags({"put", "xmpp", "notify", "send", "publish", "egress"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Sends a direct message using XMPP")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class PutXMPP extends AbstractXMPPProcessor {

    public static final PropertyDescriptor TARGET_USER = new PropertyDescriptor.Builder()
            .name("target-user")
            .displayName("Target User")
            .description("The name of the user to send the XMPP message to")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
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

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.descriptors = basePropertyDescriptorsPlus(TARGET_USER);
        this.relationships = createRelationships(SUCCESS, FAILURE);
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
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));
        validateTargetUserXorChatRoomRelationship(validationContext, results);
        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        handleFlowFile(context, session, flowFile);
    }

    private void validateTargetUserXorChatRoomRelationship(ValidationContext validationContext, List<ValidationResult> results) {
        final boolean targetUserIsSet = validationContext.getProperty(TARGET_USER).isSet();
        final boolean chatRoomIsSet = validationContext.getProperty(CHAT_ROOM).isSet();
        if (targetUserIsSet == chatRoomIsSet) {
            results.add(targetUserXorChatRoomValidationResult());
        }
    }

    private void handleFlowFile(ProcessContext context, ProcessSession session, FlowFile flowFile) {
        try {
            sendFlowFileContentsAsXmppMessage(context, session, flowFile);
        } catch (InterruptedException e) {
            handleInterruptedException(session, flowFile);
        } catch (ExecutionException e) {
            handleExecutionException(session, flowFile, e);
        }
    }

    private ValidationResult targetUserXorChatRoomValidationResult() {
        return new ValidationResult.Builder()
                .subject(PutXMPP.class.getSimpleName())
                .valid(false)
                .explanation(String.format("exactly one of '%s' or '%s' have to be set", TARGET_USER.getDisplayName(), CHAT_ROOM.getDisplayName()))
                .build();
    }

    private void sendFlowFileContentsAsXmppMessage(ProcessContext context, ProcessSession session, FlowFile flowFile)
            throws InterruptedException, ExecutionException {
        final String messageBody = getFlowFileContents(session, flowFile);
        if (chatRoom != null) {
            chatRoom.sendMessage(messageBody).get();
        } else {
            final Message message = new Message(targetJid(context), Message.Type.CHAT, messageBody);
            xmppClient.send(message).get();
        }
        session.transfer(flowFile, SUCCESS);
    }

    private String getFlowFileContents(ProcessSession session, FlowFile flowFile) {
        final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        session.exportTo(flowFile, bytes);
        return bytes.toString();
    }

    private Jid targetJid(ProcessContext context) {
        final String targetUsername = context.getProperty(TARGET_USER).getValue();
        final String xmppDomain = context.getProperty(XMPP_DOMAIN).getValue();
        return Jid.of(targetUsername + "@" + xmppDomain);
    }

    private void handleInterruptedException(ProcessSession session, FlowFile flowFile) {
        Thread.currentThread().interrupt();
        session.transfer(flowFile, SUCCESS);
    }

    private void handleExecutionException(ProcessSession session, FlowFile flowFile, ExecutionException e) {
        getLogger().error("Failed to send XMPP message", e);
        session.transfer(flowFile, FAILURE);
    }
}
