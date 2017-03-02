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
package org.apache.nifi.jms.processors;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.jms.Destination;
import javax.jms.Message;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProvider;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.StreamUtils;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.JmsHeaders;

/**
 * An implementation of JMS Message publishing {@link Processor} which upon each
 * invocation of {@link #onTrigger(ProcessContext, ProcessSession)} method will
 * construct a {@link Message} from the contents of the {@link FlowFile} sending
 * it to the {@link Destination} identified by the
 * {@link AbstractJMSProcessor#DESTINATION} property while transferring the
 * incoming {@link FlowFile} to 'success' {@link Relationship}. If message can
 * not be constructed and/or sent the incoming {@link FlowFile} will be
 * transitioned to 'failure' {@link Relationship}
 */
@Tags({ "jms", "put", "message", "send", "publish" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Creates a JMS Message from the contents of a FlowFile and sends it to a "
        + "JMS Destination (queue or topic) as JMS BytesMessage. FlowFile attributes will be added as JMS headers and/or properties to the outgoing JMS message.")
@SeeAlso(value = { ConsumeJMS.class, JMSConnectionFactoryProvider.class })
public class PublishJMS extends AbstractJMSProcessor<JMSPublisher> {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are sent to the JMS destination are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be sent to JMS destination are routed to this relationship")
            .build();

    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    /**
     * Will construct JMS {@link Message} by extracting its body from the
     * incoming {@link FlowFile}. {@link FlowFile} attributes that represent
     * standard JMS headers will be extracted from the {@link FlowFile} and set
     * as JMS headers on the newly constructed message. For the list of
     * available message headers please see {@link JmsHeaders}. <br>
     * <br>
     * Upon success the incoming {@link FlowFile} is transferred to the'success'
     * {@link Relationship} and upon failure FlowFile is penalized and
     * transferred to the 'failure' {@link Relationship}
     *
     */
    @Override
    protected void rendezvousWithJms(ProcessContext context, ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (flowFile != null) {
            try {
                String destinationName = context.getProperty(DESTINATION).evaluateAttributeExpressions(flowFile).getValue();
                this.targetResource.publish(destinationName, this.extractMessageBody(flowFile, processSession), flowFile.getAttributes());
                processSession.transfer(flowFile, REL_SUCCESS);
                processSession.getProvenanceReporter().send(flowFile, context.getProperty(DESTINATION).evaluateAttributeExpressions().getValue());
            } catch (Exception e) {
                processSession.transfer(flowFile, REL_FAILURE);
                this.getLogger().error("Failed while sending message to JMS via " + this.targetResource, e);
                context.yield();
            }
        }
    }

    /**
     *
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     * Will create an instance of {@link JMSPublisher}
     */
    @Override
    protected JMSPublisher finishBuildingTargetResource(JmsTemplate jmsTemplate, ProcessContext processContext) {
        return new JMSPublisher(jmsTemplate, this.getLogger());
    }

    /**
     * Extracts contents of the {@link FlowFile} as byte array.
     */
    private byte[] extractMessageBody(FlowFile flowFile, ProcessSession session) {
        final byte[] messageContent = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, messageContent, true);
            }
        });
        return messageContent;
    }
}
