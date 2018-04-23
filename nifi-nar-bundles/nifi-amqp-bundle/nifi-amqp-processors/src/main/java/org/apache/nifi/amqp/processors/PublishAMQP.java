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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
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
import com.rabbitmq.client.Connection;

@Tags({ "amqp", "rabbit", "put", "message", "send", "publish" })
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Creates an AMQP Message from the contents of a FlowFile and sends the message to an AMQP Exchange. "
    + "In a typical AMQP exchange model, the message that is sent to the AMQP Exchange will be routed based on the 'Routing Key' "
    + "to its final destination in the queue (the binding). If due to some misconfiguration the binding between the Exchange, Routing Key "
    + "and Queue is not set up, the message will have no final destination and will return (i.e., the data will not make it to the queue). If "
    + "that happens you will see a log in both app-log and bulletin stating to that effect, and the FlowFile will be routed to the 'failure' relationship.")
@SystemResourceConsideration(resource = SystemResource.MEMORY)
@ReadsAttributes({
    @ReadsAttribute(attribute = "amqp$appId", description = "The App ID field to set on the AMQP Message"),
    @ReadsAttribute(attribute = "amqp$contentEncoding", description = "The Content Encoding to set on the AMQP Message"),
    @ReadsAttribute(attribute = "amqp$contentType", description = "The Content Type to set on the AMQP Message"),
    @ReadsAttribute(attribute = "amqp$headers", description = "The headers to set on the AMQP Message"),
    @ReadsAttribute(attribute = "amqp$deliveryMode", description = "The numeric indicator for the Message's Delivery Mode"),
    @ReadsAttribute(attribute = "amqp$priority", description = "The Message priority"),
    @ReadsAttribute(attribute = "amqp$correlationId", description = "The Message's Correlation ID"),
    @ReadsAttribute(attribute = "amqp$replyTo", description = "The value of the Message's Reply-To field"),
    @ReadsAttribute(attribute = "amqp$expiration", description = "The Message Expiration"),
    @ReadsAttribute(attribute = "amqp$messageId", description = "The unique ID of the Message"),
    @ReadsAttribute(attribute = "amqp$timestamp", description = "The timestamp of the Message, as the number of milliseconds since epoch"),
    @ReadsAttribute(attribute = "amqp$type", description = "The type of message"),
    @ReadsAttribute(attribute = "amqp$userId", description = "The ID of the user"),
    @ReadsAttribute(attribute = "amqp$clusterId", description = "The ID of the AMQP Cluster"),
})
public class PublishAMQP extends AbstractAMQPProcessor<AMQPPublisher> {
    private static final String ATTRIBUTES_PREFIX = "amqp$";

    public static final PropertyDescriptor EXCHANGE = new PropertyDescriptor.Builder()
            .name("Exchange Name")
            .description("The name of the AMQP Exchange the messages will be sent to. Usually provided by the AMQP administrator (e.g., 'amq.direct'). "
                    + "It is an optional property. If kept empty the messages will be sent to a default AMQP exchange.")
            .required(true)
            .defaultValue("")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();
    public static final PropertyDescriptor ROUTING_KEY = new PropertyDescriptor.Builder()
            .name("Routing Key")
            .description("The name of the Routing Key that will be used by AMQP to route messages from the exchange to a destination queue(s). "
                    + "Usually provided by the administrator (e.g., 'myKey')In the event when messages are sent to a default exchange this property "
                    + "corresponds to a destination queue name, otherwise a binding from the Exchange to a Queue via Routing Key must be set "
                    + "(usually by the AMQP administrator)")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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

    static {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(EXCHANGE);
        properties.add(ROUTING_KEY);
        properties.addAll(getCommonPropertyDescriptors());
        propertyDescriptors = Collections.unmodifiableList(properties);

        Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        relationships = Collections.unmodifiableSet(rels);
    }


    /**
     * Will construct AMQP message by extracting its body from the incoming {@link FlowFile}. AMQP Properties will be extracted from the
     * {@link FlowFile} and converted to {@link BasicProperties} to be sent along with the message. Upon success the incoming {@link FlowFile} is
     * transferred to 'success' {@link Relationship} and upon failure FlowFile is penalized and transferred to the 'failure' {@link Relationship}
     * <br>
     *
     * NOTE: Attributes extracted from {@link FlowFile} are considered
     * candidates for AMQP properties if their names are prefixed with
     * {@link AMQPUtils#AMQP_PROP_PREFIX} (e.g., amqp$contentType=text/xml)
     */
    @Override
    protected void processResource(final Connection connection, final AMQPPublisher publisher, ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final BasicProperties amqpProperties = extractAmqpPropertiesFromFlowFile(flowFile);
        final String routingKey = context.getProperty(ROUTING_KEY).evaluateAttributeExpressions(flowFile).getValue();
        if (routingKey == null) {
            throw new IllegalArgumentException("Failed to determine 'routing key' with provided value '"
                + context.getProperty(ROUTING_KEY) + "' after evaluating it as expression against incoming FlowFile.");
        }

        final String exchange = context.getProperty(EXCHANGE).evaluateAttributeExpressions(flowFile).getValue();
        final byte[] messageContent = extractMessage(flowFile, session);

        try {
            publisher.publish(messageContent, amqpProperties, routingKey, exchange);
            session.transfer(flowFile, REL_SUCCESS);
            session.getProvenanceReporter().send(flowFile, connection.toString() + "/E:" + exchange + "/RK:" + routingKey);
        } catch (Exception e) {
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            getLogger().error("Failed while sending message to AMQP via " + publisher, e);
        }
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected AMQPPublisher createAMQPWorker(final ProcessContext context, final Connection connection) {
        return new AMQPPublisher(connection, getLogger());
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


    private void updateBuilderFromAttribute(final FlowFile flowFile, final String attribute, final Consumer<String> updater) {
        final String attributeValue = flowFile.getAttribute(ATTRIBUTES_PREFIX + attribute);
        if (attributeValue == null) {
            return;
        }

        try {
            updater.accept(attributeValue);
        } catch (final Exception e) {
            getLogger().warn("Failed to update AMQP Message Property " + attribute, e);
        }
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
        final AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();

        updateBuilderFromAttribute(flowFile, "contentType", builder::contentType);
        updateBuilderFromAttribute(flowFile, "contentEncoding", builder::contentEncoding);
        updateBuilderFromAttribute(flowFile, "deliveryMode", mode -> builder.deliveryMode(Integer.parseInt(mode)));
        updateBuilderFromAttribute(flowFile, "priority", pri -> builder.priority(Integer.parseInt(pri)));
        updateBuilderFromAttribute(flowFile, "correlationId", builder::correlationId);
        updateBuilderFromAttribute(flowFile, "replyTo", builder::replyTo);
        updateBuilderFromAttribute(flowFile, "expiration", builder::expiration);
        updateBuilderFromAttribute(flowFile, "messageId", builder::messageId);
        updateBuilderFromAttribute(flowFile, "timestamp", ts -> builder.timestamp(new Date(Long.parseLong(ts))));
        updateBuilderFromAttribute(flowFile, "type", builder::type);
        updateBuilderFromAttribute(flowFile, "userId", builder::userId);
        updateBuilderFromAttribute(flowFile, "appId", builder::appId);
        updateBuilderFromAttribute(flowFile, "clusterId", builder::clusterId);
        updateBuilderFromAttribute(flowFile, "headers", headers -> builder.headers(validateAMQPHeaderProperty(headers)));

        return builder.build();
    }

    /**
     * Will validate if provided amqpPropValue can be converted to a {@link Map}.
     * Should be passed in the format: amqp$headers=key=value,key=value etc.
     *
     * @param amqpPropValue the value of the property
     * @return {@link Map} if valid otherwise null
     */
    private Map<String, Object> validateAMQPHeaderProperty(String amqpPropValue) {
        String[] strEntries = amqpPropValue.split(",");
        Map<String, Object> headers = new HashMap<>();
        for (String strEntry : strEntries) {
            String[] kv = strEntry.split("=");
            if (kv.length == 2) {
                headers.put(kv[0].trim(), kv[1].trim());
            } else {
                getLogger().warn("Malformed key value pair for AMQP header property: " + amqpPropValue);
            }
        }

        return headers;
    }
}
