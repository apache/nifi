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
package org.apache.nifi.processor.util.listen;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

/**
 * Shared properties.
 */
public class ListenerProperties {

    private static final Set<String> interfaceSet = new HashSet<>();

    static {
        try {
            final Enumeration<NetworkInterface> interfaceEnum = NetworkInterface.getNetworkInterfaces();
            while (interfaceEnum.hasMoreElements()) {
                final NetworkInterface ifc = interfaceEnum.nextElement();
                interfaceSet.add(ifc.getName());
            }
        } catch (SocketException ignored) {
        }
    }

    public static final PropertyDescriptor NETWORK_INTF_NAME = new PropertyDescriptor.Builder()
            .name("Local Network Interface")
            .description("The name of a local network interface to be used to restrict listening to a specific LAN.")
            .addValidator((subject, input, context) -> {
                ValidationResult result = new ValidationResult.Builder()
                        .subject("Local Network Interface").valid(true).input(input).build();
                if (interfaceSet.contains(input.toLowerCase())) {
                    return result;
                }

                String message;
                String realValue = input;
                try {
                    if (context.isExpressionLanguagePresent(input)) {
                        AttributeExpression ae = context.newExpressionLanguageCompiler().compile(input);
                        realValue = ae.evaluate();
                    }

                    if (interfaceSet.contains(realValue.toLowerCase())) {
                        return result;
                    }

                    message = realValue + " is not a valid network name. Valid names are " + interfaceSet;

                } catch (IllegalArgumentException e) {
                    message = "Not a valid AttributeExpression: " + e.getMessage();
                }
                result = new ValidationResult.Builder().subject("Local Network Interface")
                        .valid(false).input(input).explanation(message).build();

                return result;
            })
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor
            .Builder().name("Port")
            .description("The port to listen on for communication.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set of the received data.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();
    public static final PropertyDescriptor RECV_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Receive Buffer Size")
            .description("The size of each buffer used to receive messages. Adjust this value appropriately based on the expected size of the " +
                    "incoming messages.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("65507 B")
            .required(true)
            .build();
    public static final PropertyDescriptor MAX_SOCKET_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Max Size of Socket Buffer")
            .description("The maximum size of the socket buffer that should be used. This is a suggestion to the Operating System " +
                    "to indicate how big the socket buffer should be. If this value is set too low, the buffer may fill up before " +
                    "the data can be read, and incoming data will be dropped.")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .required(true)
            .build();
    public static final PropertyDescriptor MAX_MESSAGE_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .name("Max Size of Message Queue")
            .description("The maximum size of the internal queue used to buffer messages being transferred from the underlying channel to the processor. " +
                    "Setting this value higher allows more messages to be buffered in memory during surges of incoming messages, but increases the total " +
                    "memory used by the processor during these surges.")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .defaultValue("10000")
            .required(true)
            .build();
    public static final PropertyDescriptor WORKER_THREADS = new PropertyDescriptor.Builder()
            .name("Max Number of TCP Connections")
            .displayName("Max Number of Worker Threads")
            .description("The maximum number of worker threads available for servicing TCP connections.")
            .addValidator(StandardValidators.createLongValidator(1, 65535, true))
            .defaultValue("2")
            .required(true)
            .build();
    public static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Max Batch Size")
            .description(
                    "The maximum number of messages to add to a single FlowFile. If multiple messages are available, they will be concatenated along with "
                            + "the <Message Delimiter> up to this configured maximum number of messages")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .defaultValue("1")
            .required(true)
            .build();
    public static final PropertyDescriptor MESSAGE_DELIMITER = new PropertyDescriptor.Builder()
            .name("Message Delimiter")
            .displayName("Batching Message Delimiter")
            .description("Specifies the delimiter to place between messages when multiple messages are bundled together (see <Max Batch Size> property).")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .defaultValue("\\n")
            .required(true)
            .build();

}
