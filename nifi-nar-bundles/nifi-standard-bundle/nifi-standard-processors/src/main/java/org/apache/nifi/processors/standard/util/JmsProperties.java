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
package org.apache.nifi.processors.standard.util;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

public class JmsProperties {

    public static final String ACTIVEMQ_PROVIDER = "ActiveMQ";

    public static final String ACK_MODE_CLIENT = "Client Acknowledge";
    public static final String ACK_MODE_AUTO = "Auto Acknowledge";

    public static final String DESTINATION_TYPE_QUEUE = "Queue";
    public static final String DESTINATION_TYPE_TOPIC = "Topic";

    public static final String MSG_TYPE_BYTE = "byte";
    public static final String MSG_TYPE_TEXT = "text";
    public static final String MSG_TYPE_STREAM = "stream";
    public static final String MSG_TYPE_MAP = "map";
    public static final String MSG_TYPE_EMPTY = "empty";

    // Standard JMS Properties
    public static final PropertyDescriptor JMS_PROVIDER = new PropertyDescriptor.Builder()
            .name("JMS Provider")
            .description("The Provider used for the JMS Server")
            .required(true)
            .allowableValues(ACTIVEMQ_PROVIDER)
            .defaultValue(ACTIVEMQ_PROVIDER)
            .build();
    public static final PropertyDescriptor URL = new PropertyDescriptor.Builder()
            .name("URL")
            .description("The URL of the JMS Server")
            .addValidator(StandardValidators.URI_VALIDATOR)
            .required(true)
            .build();
    public static final PropertyDescriptor TIMEOUT = new PropertyDescriptor.Builder()
            .name("Communications Timeout")
            .description("The amount of time to wait when attempting to receive a message before giving up and assuming failure")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .defaultValue("30 sec")
            .build();
    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("Username used for authentication and authorization")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Password used for authentication and authorization")
            .required(false)
            .addValidator(Validator.VALID)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor CLIENT_ID_PREFIX = new PropertyDescriptor.Builder()
            .name("Client ID Prefix")
            .description("A human-readable ID that can be used to associate connections with yourself so that the maintainers of the JMS Server know who to contact if problems arise")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // Topic/Queue determination Properties
    public static final PropertyDescriptor DESTINATION_NAME = new PropertyDescriptor.Builder()
            .name("Destination Name")
            .description("The name of the JMS Topic or queue to use")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor DESTINATION_TYPE = new PropertyDescriptor.Builder()
            .name("Destination Type")
            .description("The type of the JMS Destination to use")
            .required(true)
            .allowableValues(DESTINATION_TYPE_QUEUE, DESTINATION_TYPE_TOPIC)
            .defaultValue(DESTINATION_TYPE_QUEUE)
            .build();

    public static final PropertyDescriptor DURABLE_SUBSCRIPTION = new PropertyDescriptor.Builder()
            .name("Use Durable Subscription")
            .description("If true, connections to the specified topic will use Durable Subscription so that messages are queued when we are not pulling them")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    // JMS Publisher Properties
    public static final PropertyDescriptor ATTRIBUTES_TO_JMS_PROPS = new PropertyDescriptor.Builder()
            .name("Copy Attributes to JMS Properties")
            .description("Whether or not FlowFile Attributes should be translated into JMS Message Properties. If true, all "
                    + "attributes starting with 'jms.' will be set as Properties on the JMS Message (without the 'jms.' prefix). "
                    + "If an attribute exists that starts with the same value but ends in '.type', that attribute will be used "
                    + "to determine the JMS Message Property type.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    // JMS Listener Properties
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Message Batch Size")
            .description("The number of messages to pull/push in a single iteration of the processor")
            .required(true)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10")
            .build();
    public static final PropertyDescriptor ACKNOWLEDGEMENT_MODE = new PropertyDescriptor.Builder()
            .name("Acknowledgement Mode")
            .description("The JMS Acknowledgement Mode. Using Auto Acknowledge can cause messages to be lost on restart of NiFi but may provide better performance than Client Acknowledge.")
            .required(true)
            .allowableValues(ACK_MODE_CLIENT, ACK_MODE_AUTO)
            .defaultValue(ACK_MODE_CLIENT)
            .build();
    public static final PropertyDescriptor JMS_PROPS_TO_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("Copy JMS Properties to Attributes")
            .description("Whether or not the JMS Message Properties should be copied to the FlowFile Attributes; if so, the attribute name will be jms.XXX, where XXX is the JMS Property name")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();
    public static final PropertyDescriptor MESSAGE_SELECTOR = new PropertyDescriptor.Builder()
            .name("Message Selector")
            .description("The JMS Message Selector to use in order to narrow the messages that are pulled")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // JMS Producer Properties
    public static final PropertyDescriptor MESSAGE_TYPE = new PropertyDescriptor.Builder()
            .name("Message Type")
            .description("The Type of JMS Message to Construct")
            .required(true)
            .allowableValues(MSG_TYPE_BYTE, MSG_TYPE_STREAM, MSG_TYPE_TEXT, MSG_TYPE_MAP, MSG_TYPE_EMPTY)
            .defaultValue(MSG_TYPE_BYTE)
            .build();
    public static final PropertyDescriptor MESSAGE_PRIORITY = new PropertyDescriptor.Builder()
            .name("Message Priority")
            .description("The Priority of the Message")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor REPLY_TO_QUEUE = new PropertyDescriptor.Builder()
            .name("Reply-To Queue")
            .description("The name of the queue to which a reply to should be added")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();
    public static final PropertyDescriptor MESSAGE_TTL = new PropertyDescriptor.Builder()
            .name("Message Time to Live")
            .description("The amount of time that the message should live on the destination before being removed; if not specified, the message will never expire.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();
    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Max Buffer Size")
            .description("The maximum amount of data that can be buffered for a JMS Message. If a FlowFile's size exceeds this value, the FlowFile will be routed to failure.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();

    // JMS SSL Properties
    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The Controller Service to use in order to obtain an SSL Context.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();
}
