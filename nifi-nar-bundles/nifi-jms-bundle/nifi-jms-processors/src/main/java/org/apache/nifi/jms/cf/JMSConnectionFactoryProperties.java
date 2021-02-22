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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextService;

import java.util.Arrays;
import java.util.List;

public class JMSConnectionFactoryProperties {

    private static final String BROKER = "broker";
    private static final String CF_IMPL = "cf";
    private static final String CF_LIB = "cflib";

    public static final PropertyDescriptor JMS_CONNECTION_FACTORY_IMPL = new PropertyDescriptor.Builder()
            .name(CF_IMPL)
            .displayName("JMS Connection Factory Implementation Class")
            .description("The fully qualified name of the JMS ConnectionFactory implementation "
                    + "class (eg. org.apache.activemq.ActiveMQConnectionFactory).")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor JMS_CLIENT_LIBRARIES = new PropertyDescriptor.Builder()
            .name(CF_LIB)
            .displayName("JMS Client Libraries")
            .description("Path to the directory with additional resources (eg. JARs, configuration files etc.) to be added "
                    + "to the classpath (defined as a comma separated list of values). Such resources typically represent target JMS client libraries "
                    + "for the ConnectionFactory implementation.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY, ResourceType.URL)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor JMS_BROKER_URI = new PropertyDescriptor.Builder()
            .name(BROKER)
            .displayName("JMS Broker URI")
            .description("URI pointing to the network location of the JMS Message broker. Example for ActiveMQ: "
                    + "'tcp://myhost:61616'. Examples for IBM MQ: 'myhost(1414)' and 'myhost01(1414),myhost02(1414)'.")
            .required(false)
            .addValidator(new NonEmptyBrokerURIValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor JMS_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .displayName("JMS SSL Context Service")
            .description("The SSL Context Service used to provide client certificate information for TLS/SSL connections.")
            .required(false)
            .identifiesControllerService(SSLContextService.class)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Arrays.asList(
            JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL,
            JMSConnectionFactoryProperties.JMS_CLIENT_LIBRARIES,
            JMSConnectionFactoryProperties.JMS_BROKER_URI,
            JMSConnectionFactoryProperties.JMS_SSL_CONTEXT_SERVICE
    );

    public static List<PropertyDescriptor> getPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    public static PropertyDescriptor getDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName
                        + "' property to be set on the provided Connection Factory implementation.")
                .name(propertyDescriptorName)
                .required(false)
                .dynamic(true)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .build();
    }

    /**
     * {@link Validator} that ensures that brokerURI's length > 0 after EL
     * evaluation
     */
    private static class NonEmptyBrokerURIValidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }
            return StandardValidators.NON_EMPTY_VALIDATOR.validate(subject, input, context);
        }
    }

}
