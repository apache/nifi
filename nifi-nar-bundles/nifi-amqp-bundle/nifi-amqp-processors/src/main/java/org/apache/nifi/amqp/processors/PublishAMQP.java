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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * Publishing AMQP processor which upon each invocation of
 * {@link #onTrigger(ProcessContext, ProcessSession)} method will construct an
 * AMQP message sending it to an exchange identified during construction of this
 * class while transferring the incoming {@link FlowFile} to 'success'
 * {@link Relationship}.
 *
 * Expects that queues, exchanges and bindings are pre-defined by an AMQP
 * administrator
 */
@Tags({ "amqp", "rabbit", "put", "message", "send", "publish" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Creates a AMQP Message from the contents of a FlowFile and sends the message to an AMQP Exchange."
        + "In a typical AMQP exchange model, the message that is sent to the AMQP Exchange will be routed based on the 'Routing Key' "
        + "to its final destination in the queue (the binding). If due to some misconfiguration the binding between the Exchange, Routing Key "
        + "and Queue is not set up, the message will have no final destination and will return (i.e., the data will not make it to the queue). If "
        + "that happens you will see a log in both app-log and bulletin stating to that effect. Fixing the binding "
        + "(normally done by AMQP administrator) will resolve the issue.")
public class PublishAMQP extends AbstractAMQPProcessor<AMQPPublisher> {

    public static final PropertyDescriptor EXCHANGE = new PropertyDescriptor.Builder()
            .name("Exchange Name")
            .description("The name of the AMQP Exchange the messages will be sent to. Usually provided by the AMQP administrator (e.g., 'amq.direct'). "
                    + "It is an optional property. If kept empty the messages will be sent to a default AMQP exchange.")
            .required(true)
            .defaultValue("")
            .expressionLanguageSupported(true)
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor ROUTING_KEY = new PropertyDescriptor.Builder()
            .name("Routing Key")
            .description("The name of the Routing Key that will be used by AMQP to route messages from the exchange to a destination queue(s). "
                    + "Usually provided by the administrator (e.g., 'myKey')In the event when messages are sent to a default exchange this property "
                    + "corresponds to a destination queue name, otherwise a binding from the Exchange to a Queue via Routing Key must be set "
                    + "(usually by the AMQP administrator)")
            .required(true)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are sent to the AMQP destination are routed to this relationship")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All FlowFiles that cannot be routed to the AMQP destination are routed to this relationship")
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors;

    private final static Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(EXCHANGE);
        _propertyDescriptors.add(ROUTING_KEY);
        _propertyDescriptors.addAll(descriptors);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    /**
     * Will construct AMQP message by extracting its body from the incoming
     * {@link FlowFile}. AMQP Properties will be extracted from the
     * {@link FlowFile} and converted to {@link BasicProperties} to be sent
     * along with the message. Upon success the incoming {@link FlowFile} is
     * transferred to 'success' {@link Relationship} and upon failure FlowFile is
     * penalized and transferred to the 'failure' {@link Relationship}
     * <br>
     * NOTE: Attributes extracted from {@link FlowFile} are considered
     * candidates for AMQP properties if their names are prefixed with
     * {@link AMQPUtils#AMQP_PROP_PREFIX} (e.g., amqp$contentType=text/xml)
     *
     */
    @Override
    protected void rendezvousWithAmqp(ProcessContext context, ProcessSession processSession) throws ProcessException {
        FlowFile flowFile = processSession.get();
        if (flowFile != null) {
            BasicProperties amqpProperties = this.extractAmqpPropertiesFromFlowFile(flowFile);
            String routingKey = context.getProperty(ROUTING_KEY).evaluateAttributeExpressions(flowFile).getValue();
            if (routingKey == null){
                throw new IllegalArgumentException("Failed to determine 'routing key' with provided value '"
                        + context.getProperty(ROUTING_KEY) + "' after evaluating it as expression against incoming FlowFile.");
            }
            String exchange = context.getProperty(EXCHANGE).evaluateAttributeExpressions(flowFile).getValue();

            byte[] messageContent = this.extractMessage(flowFile, processSession);

            try {
                this.targetResource.publish(messageContent, amqpProperties, routingKey, exchange);
                processSession.transfer(flowFile, REL_SUCCESS);
                processSession.getProvenanceReporter().send(flowFile, this.amqpConnection.toString() + "/E:" + exchange + "/RK:" + routingKey);
            } catch (Exception e) {
                processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
                this.getLogger().error("Failed while sending message to AMQP via " + this.targetResource, e);
                context.yield();
            }
        }
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

    /**
     * Will create an instance of {@link AMQPPublisher}
     */
    @Override
    protected AMQPPublisher finishBuildingTargetResource(ProcessContext context) {
        return new AMQPPublisher(this.amqpConnection, this.getLogger());
    }

    /**
     * Extracts contents of the {@link FlowFile} as byte array.
     */
    private byte[] extractMessage(FlowFile flowFile, ProcessSession session){
        final byte[] messageContent = new byte[(int) flowFile.getSize()];
        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                StreamUtils.fillBuffer(in, messageContent, true);
            }
        });
        return messageContent;
    }

    /**
     * Extracts AMQP properties from the {@link FlowFile} attributes. Attributes
     * extracted from {@link FlowFile} are considered candidates for AMQP
     * properties if their names are prefixed with
     * {@link AMQPUtils#AMQP_PROP_PREFIX} (e.g., amqp$contentType=text/xml).
     *
     * Some fields require a specific format and are validated:
     *
     * {@link AMQPUtils#validateAMQPHeaderProperty}
     * {@link AMQPUtils#validateAMQPDeliveryModeProperty}
     * {@link AMQPUtils#validateAMQPPriorityProperty}
     * {@link AMQPUtils#validateAMQPTimestampProperty}
     */
    private BasicProperties extractAmqpPropertiesFromFlowFile(FlowFile flowFile) {
        Map<String, String> attributes = flowFile.getAttributes();
        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
        for (Entry<String, String> attributeEntry : attributes.entrySet()) {
            if (attributeEntry.getKey().startsWith(AMQPUtils.AMQP_PROP_PREFIX)) {
                String amqpPropName = attributeEntry.getKey();
                String amqpPropValue = attributeEntry.getValue();

                AMQPUtils.PropertyNames propertyNames = AMQPUtils.PropertyNames.fromValue(amqpPropName);

                if (propertyNames != null) {
                    switch (propertyNames){
                        case CONTENT_TYPE:
                            builder.contentType(amqpPropValue);
                            break;
                        case CONTENT_ENCODING:
                            builder.contentEncoding(amqpPropValue);
                            break;
                        case HEADERS:
                            builder.headers(AMQPUtils.validateAMQPHeaderProperty(amqpPropValue));
                            break;
                        case DELIVERY_MODE:
                            builder.deliveryMode(AMQPUtils.validateAMQPDeliveryModeProperty(amqpPropValue));
                            break;
                        case PRIORITY:
                            builder.priority(AMQPUtils.validateAMQPPriorityProperty(amqpPropValue));
                            break;
                        case CORRELATION_ID:
                            builder.correlationId(amqpPropValue);
                            break;
                        case REPLY_TO:
                            builder.replyTo(amqpPropValue);
                            break;
                        case EXPIRATION:
                            builder.expiration(amqpPropValue);
                            break;
                        case MESSAGE_ID:
                            builder.messageId(amqpPropValue);
                            break;
                        case TIMESTAMP:
                            builder.timestamp(AMQPUtils.validateAMQPTimestampProperty(amqpPropValue));
                            break;
                        case TYPE:
                            builder.type(amqpPropValue);
                            break;
                        case USER_ID:
                            builder.userId(amqpPropValue);
                            break;
                        case APP_ID:
                            builder.appId(amqpPropValue);
                            break;
                        case CLUSTER_ID:
                            builder.clusterId(amqpPropValue);
                            break;
                    }

                } else {
                    getLogger().warn("Unrecognised AMQP property '" + amqpPropName + "', will ignore.");
                }
            }
        }
        return builder.build();
    }
}
