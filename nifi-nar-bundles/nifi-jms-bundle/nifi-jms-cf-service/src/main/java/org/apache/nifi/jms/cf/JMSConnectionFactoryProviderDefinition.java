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
package org.apache.nifi.jms.cf;

import java.io.File;

import javax.jms.ConnectionFactory;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

/**
 * Defines a strategy to create implementations to load and initialize third
 * party implementations of the {@link ConnectionFactory}
 */
public interface JMSConnectionFactoryProviderDefinition extends ControllerService {
    static final String BROKER = "broker";
    static final String CF_IMPL = "cf";
    static final String CF_LIB = "cflib";

    public static final PropertyDescriptor CONNECTION_FACTORY_IMPL = new PropertyDescriptor.Builder()
            .name(CF_IMPL)
            .displayName("MQ ConnectionFactory Implementation")
            .description("A fully qualified name of the JMS ConnectionFactory implementation "
                       + "class (i.e., org.apache.activemq.ActiveMQConnectionFactory)")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(true)
            .build();
    public static final PropertyDescriptor CLIENT_LIB_DIR_PATH = new PropertyDescriptor.Builder()
            .name(CF_LIB)
            .displayName("MQ Client Libraries path (i.e., /usr/jms/lib)")
            .description("Path to the directory with additional resources (i.e., JARs, configuration files etc.) to be added "
                       + "to the classpath. Such resources typically represent target MQ client libraries for the "
                       + "ConnectionFactory implementation.")
            .addValidator(new ClientLibValidator())
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    // ConnectionFactory specific properties
    public static final PropertyDescriptor BROKER_URI = new PropertyDescriptor.Builder()
            .name(BROKER)
            .displayName("Broker URI")
            .description("URI pointing to the network location of the JMS Message broker. For example, "
                      + "'tcp://myhost:61616' for ActiveMQ or 'myhost:1414' for IBM MQ")
            .addValidator(new NonEmptyBrokerURIValidator())
            .required(true)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    /**
     * Returns an instance of the {@link ConnectionFactory} specific to the
     * target messaging system (i.e.,
     * org.apache.activemq.ActiveMQConnectionFactory). It is created based on
     * the value of the supplied 'CONNECTION_FACTORY_IMPL' property and JMS
     * client libraries supplied via 'CLIENT_LIB_DIR_PATH' property.
     *
     * @return instance of {@link ConnectionFactory}
     */
    ConnectionFactory getConnectionFactory();

    /**
     * {@link Validator} that ensures that brokerURI's length > 0 after EL evaluation
     */
    static class NonEmptyBrokerURIValidator implements Validator {

        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            String value = input;
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                value = context.getProperty(BROKER_URI).evaluateAttributeExpressions().getValue();
            }
            return StandardValidators.NON_EMPTY_VALIDATOR.validate(subject, value, context);
        }
    }

    /**
     *
     */
    static class ClientLibValidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            String libDirPath = context.getProperty(CLIENT_LIB_DIR_PATH).evaluateAttributeExpressions().getValue();
            StringBuilder invalidationMessageBuilder = new StringBuilder();
            if (libDirPath != null) {
                File file = new File(libDirPath);
                if (!file.isDirectory()) {
                    invalidationMessageBuilder.append("'MQ Client Libraries path' must point to a directory. Was '"
                            + file.getAbsolutePath() + "'.");
                }
            } else {
                invalidationMessageBuilder.append("'MQ Client Libraries path' must be provided. \n");
            }
            String invalidationMessage = invalidationMessageBuilder.toString();
            ValidationResult vResult;
            if (invalidationMessage.length() == 0) {
                vResult = new ValidationResult.Builder().subject(subject).input(input)
                        .explanation("Client lib path is valid and points to a directory").valid(true).build();
            } else {
                vResult = new ValidationResult.Builder().subject(subject).input(input)
                        .explanation("Client lib path is invalid. " + invalidationMessage)
                        .valid(false).build();
            }
            return vResult;
        }
    }
}
