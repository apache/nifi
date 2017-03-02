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
package org.apache.nifi.amqp.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.GetResponse;

/**
 * Consuming AMQP processor which upon each invocation of
 * {@link #onTrigger(ProcessContext, ProcessSession)} method will construct a
 * {@link FlowFile} containing the body of the consumed AMQP message and AMQP
 * properties that came with message which are added to a {@link FlowFile} as
 * attributes.
 */
@Tags({ "amqp", "rabbit", "get", "message", "receive", "consume" })
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("Consumes AMQP Message transforming its content to a FlowFile and transitioning it to 'success' relationship")
public class ConsumeAMQP extends AbstractAMQPProcessor<AMQPConsumer> {

    public static final PropertyDescriptor QUEUE = new PropertyDescriptor.Builder()
            .name("Queue")
            .description("The name of the existing AMQP Queue from which messages will be consumed. Usually pre-defined by AMQP administrator. ")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received from the AMQP queue are routed to this relationship")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;

    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(QUEUE);
        _propertyDescriptors.addAll(descriptors);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    /**
     * Will construct a {@link FlowFile} containing the body of the consumed
     * AMQP message (if {@link GetResponse} returned by {@link AMQPConsumer} is
     * not null) and AMQP properties that came with message which are added to a
     * {@link FlowFile} as attributes, transferring {@link FlowFile} to
     * 'success' {@link Relationship}.
     */
    @Override
    protected void rendezvousWithAmqp(ProcessContext context, ProcessSession processSession) throws ProcessException {
        final GetResponse response = this.targetResource.consume();
        if (response != null){
            FlowFile flowFile = processSession.create();
            flowFile = processSession.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(response.getBody());
                }
            });
            BasicProperties amqpProperties = response.getProps();
            flowFile = AMQPUtils.updateFlowFileAttributesWithAmqpProperties(amqpProperties, flowFile, processSession);
            processSession.getProvenanceReporter().receive(flowFile,
                    this.amqpConnection.toString() + "/" + context.getProperty(QUEUE).getValue());
            processSession.transfer(flowFile, REL_SUCCESS);
        } else {
            context.yield();
        }
    }

    /**
     * Will create an instance of {@link AMQPConsumer}
     */
    @Override
    protected AMQPConsumer finishBuildingTargetResource(ProcessContext context) {
        String queueName = context.getProperty(QUEUE).getValue();
        return new AMQPConsumer(this.amqpConnection, queueName);
    }

    /**
     *
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    /**
     *
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
}
