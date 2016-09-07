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
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.jms.cf.JMSConnectionFactoryProvider;
import org.apache.nifi.jms.processors.JMSConsumer.JMSResponse;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.springframework.jms.core.JmsTemplate;

/**
 * Consuming JMS processor which upon each invocation of
 * {@link #onTrigger(ProcessContext, ProcessSession)} method will construct a
 * {@link FlowFile} containing the body of the consumed JMS message and JMS
 * properties that came with message which are added to a {@link FlowFile} as
 * attributes.
 */
@Tags({ "jms", "get", "message", "receive", "consume" })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Consumes JMS Message of type BytesMessage or TextMessage transforming its content to "
        + "a FlowFile and transitioning it to 'success' relationship.")
@SeeAlso(value = { PublishJMS.class, JMSConnectionFactoryProvider.class })
public class ConsumeJMS extends AbstractJMSProcessor<JMSConsumer> {

    public static final String JMS_SOURCE_DESTINATION_NAME = "jms.source.destination";

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received from the JMS Destination are routed to this relationship")
            .build();

    private final static Set<Relationship> relationships;

    static {
        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    /**
     * Will construct a {@link FlowFile} containing the body of the consumed JMS
     * message (if {@link GetResponse} returned by {@link JMSConsumer} is not
     * null) and JMS properties that came with message which are added to a
     * {@link FlowFile} as attributes, transferring {@link FlowFile} to
     * 'success' {@link Relationship}.
     */
    @Override
    protected void rendezvousWithJms(ProcessContext context, ProcessSession processSession) throws ProcessException {
        final JMSResponse response = this.targetResource.consume();
        if (response != null){
            FlowFile flowFile = processSession.create();
            flowFile = processSession.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(response.getMessageBody());
                }
            });
            Map<String, Object> jmsHeaders = response.getMessageHeaders();
            flowFile = this.updateFlowFileAttributesWithJmsHeaders(jmsHeaders, flowFile, processSession);
            processSession.getProvenanceReporter().receive(flowFile, context.getProperty(DESTINATION).evaluateAttributeExpressions().getValue());
            processSession.transfer(flowFile, REL_SUCCESS);
        } else {
            context.yield();
        }
    }

    /**
     * Will create an instance of {@link JMSConsumer}
     */
    @Override
    protected JMSConsumer finishBuildingTargetResource(JmsTemplate jmsTemplate) {
        return new JMSConsumer(jmsTemplate, this.getLogger());
    }

    /**
     *
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /**
     *
     */
    private FlowFile updateFlowFileAttributesWithJmsHeaders(Map<String, Object> jmsHeaders, FlowFile flowFile, ProcessSession processSession) {
        Map<String, String> attributes = new HashMap<String, String>();
        for (Entry<String, Object> headersEntry : jmsHeaders.entrySet()) {
            attributes.put(headersEntry.getKey(), String.valueOf(headersEntry.getValue()));
        }
        attributes.put(JMS_SOURCE_DESTINATION_NAME, this.destinationName);
        flowFile = processSession.putAllAttributes(flowFile, attributes);
        return flowFile;
    }
}
