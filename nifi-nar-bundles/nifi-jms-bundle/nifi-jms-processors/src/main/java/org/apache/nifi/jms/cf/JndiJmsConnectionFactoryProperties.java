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
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

public class JndiJmsConnectionFactoryProperties {

    public static final PropertyDescriptor JNDI_INITIAL_CONTEXT_FACTORY = new Builder()
            .name("java.naming.factory.initial")
            .displayName("JNDI Initial Context Factory Class")
            .description("The fully qualified class name of the JNDI Initial Context Factory Class (java.naming.factory.initial).")
            .required(true)
            .addValidator(new JndiJmsContextFactoryValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor JNDI_PROVIDER_URL = new Builder()
            .name("java.naming.provider.url")
            .displayName("JNDI Provider URL")
            .description("The URL of the JNDI Provider to use (java.naming.provider.url).")
            .required(true)
            .addValidator(new JndiJmsProviderUrlValidator())
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

    private static class JndiJmsContextFactoryValidator implements Validator {
        private static final String DISALLOWED_CONTEXT_FACTORY = "LdapCtxFactory";

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder().subject(subject).input(input);

            if (input == null || input.isEmpty()) {
                builder.valid(false);
                builder.explanation("Context Factory is required");
            } else if (input.endsWith(DISALLOWED_CONTEXT_FACTORY)) {
                builder.valid(false);
                builder.explanation(String.format("Context Factory [%s] not allowed", DISALLOWED_CONTEXT_FACTORY));
            } else {
                builder.valid(true);
                builder.explanation("Context Factory allowed");
            }

            return builder.build();
        }
    }

    private static class JndiJmsProviderUrlValidator implements Validator {
        /** JNDI JMS URL Allowed Schemes based on ActiveMQ Connection Factory */
        private static final Set<String> ALLOWED_SCHEMES = Collections.unmodifiableSet(new LinkedHashSet<>(Arrays.asList(
                "jgroups",
                "tcp",
                "udp",
                "vm"
        )));

        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            final ValidationResult.Builder builder = new ValidationResult.Builder().subject(subject).input(input);

            if (input == null || input.isEmpty()) {
                builder.valid(false);
                builder.explanation("URL is required");
            } else if (startsWithAllowedScheme(input)) {
                builder.valid(true);
                builder.explanation("URL scheme allowed");
            } else {
                builder.valid(false);
                final String explanation = String.format("URL scheme not allowed. Allowed URL schemes include %s", ALLOWED_SCHEMES);
                builder.explanation(explanation);
            }

            return builder.build();
        }

        private boolean startsWithAllowedScheme(final String input) {
            boolean startsWithAllowedScheme = false;

            for (final String allowedScheme : ALLOWED_SCHEMES) {
                if (input.startsWith(allowedScheme)) {
                    startsWithAllowedScheme = true;
                    break;
                }
            }

            return startsWithAllowedScheme;
        }
    }
}
