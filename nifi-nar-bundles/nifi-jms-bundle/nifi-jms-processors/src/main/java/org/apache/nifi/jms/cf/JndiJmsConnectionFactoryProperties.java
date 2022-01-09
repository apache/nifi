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
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.List;

import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

public class JndiJmsConnectionFactoryProperties {

    public static final PropertyDescriptor JNDI_INITIAL_CONTEXT_FACTORY = new Builder()
            .name("java.naming.factory.initial")
            .displayName("JNDI Initial Context Factory Class")
            .description("The fully qualified class name of the JNDI Initial Context Factory Class (java.naming.factory.initial).")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor JNDI_PROVIDER_URL = new Builder()
            .name("java.naming.provider.url")
            .displayName("JNDI Provider URL")
            .description("The URL of the JNDI Provider to use (java.naming.provider.url).")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor JNDI_CONNECTION_FACTORY_NAME = new Builder()
            .name("connection.factory.name")
            .displayName("JNDI Name of the Connection Factory")
            .description("The name of the JNDI Object to lookup for the Connection Factory.")
            .required(true)
            .addValidator(NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor JNDI_CLIENT_LIBRARIES = new Builder()
            .name("naming.factory.libraries")
            .displayName("JNDI / JMS Client Libraries")
            .description("Specifies jar files and/or directories to add to the ClassPath " +
                    "in order to load the JNDI / JMS client libraries. This should be a comma-separated list of files, directories, and/or URLs. If a directory is given, any files in that directory" +
                    " will be included, but subdirectories will not be included (i.e., it is not recursive).")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY, ResourceType.URL)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor JNDI_PRINCIPAL = new Builder()
            .name("java.naming.security.principal")
            .displayName("JNDI Principal")
            .description("The Principal to use when authenticating with JNDI (java.naming.security.principal).")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor JNDI_CREDENTIALS = new Builder()
            .name("java.naming.security.credentials")
            .displayName("JNDI Credentials")
            .description("The Credentials to use when authenticating with JNDI (java.naming.security.credentials).")
            .required(false)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .sensitive(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Arrays.asList(
            JndiJmsConnectionFactoryProperties.JNDI_INITIAL_CONTEXT_FACTORY,
            JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL,
            JndiJmsConnectionFactoryProperties.JNDI_CONNECTION_FACTORY_NAME,
            JndiJmsConnectionFactoryProperties.JNDI_CLIENT_LIBRARIES,
            JndiJmsConnectionFactoryProperties.JNDI_PRINCIPAL,
            JndiJmsConnectionFactoryProperties.JNDI_CREDENTIALS
    );

    public static List<PropertyDescriptor> getPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    public static PropertyDescriptor getDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new Builder()
                .name(propertyDescriptorName)
                .displayName(propertyDescriptorName)
                .description("JNDI Initial Context Environment configuration for '" + propertyDescriptorName + "'")
                .required(false)
                .dynamic(true)
                .addValidator(Validator.VALID)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .build();
    }

}
