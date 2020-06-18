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
package org.apache.nifi.hazelcast.services.cachemanager;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"hazelcast", "cache"})
@CapabilityDescription("A service that provides connections to an embedded Hazelcast instance started by NiFi.")
public class EmbeddedHazelcastCacheManager extends IMapBasedHazelcastCacheManager {

    static final PropertyDescriptor HAZELCAST_PORT = new PropertyDescriptor.Builder()
            .name("hazelcast-port")
            .displayName("Hazelcast Port")
            .description("Port the Hazelcast uses as starting port. If not specified, the default value will be used, which is 5701")
            .required(false)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor HAZELCAST_PORT_COUNT = new PropertyDescriptor.Builder()
            .name("hazelcast-port-count")
            .displayName("Hazelcast Port Count")
            // With the current version, the default is 100
            .description("The maximum number of ports the Hazelcast uses. If not specified, the default will be used")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor HAZELCAST_PORT_AUTO_INCREMENT = new PropertyDescriptor.Builder()
            .name("hazelcast-port-auto-increment")
            .displayName("Hazelcast Port Auto Increment")
            .description("Might turn on Hazelcast's auto increment port feature.")
            .required(false)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    static {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HAZELCAST_INSTANCE_NAME);
        properties.add(HAZELCAST_CLUSTER_NAME);
        properties.add(HAZELCAST_PORT);
        properties.add(HAZELCAST_PORT_COUNT);
        properties.add(HAZELCAST_PORT_AUTO_INCREMENT);
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(properties);
    }

    @Override
    protected HazelcastInstance getInstance(final ConfigurationContext context) {
        final Config config = new Config(context.getProperty(HAZELCAST_INSTANCE_NAME).getValue());

        if (context.getProperty(HAZELCAST_CLUSTER_NAME).isSet()) {
            config.setClusterName(context.getProperty(HAZELCAST_CLUSTER_NAME).getValue());
        }

        if (context.getProperty(HAZELCAST_PORT).isSet()) {
            final NetworkConfig networkConfig = config.getNetworkConfig();
            networkConfig.setPort(context.getProperty(HAZELCAST_PORT).asInteger());

            if (context.getProperty(HAZELCAST_PORT_COUNT).isSet()) {
                networkConfig.setPortCount(context.getProperty(HAZELCAST_PORT_COUNT).asInteger());
            }

            if (context.getProperty(HAZELCAST_PORT_AUTO_INCREMENT).isSet()) {
                networkConfig.setPortAutoIncrement(context.getProperty(HAZELCAST_PORT_AUTO_INCREMENT).asBoolean());
            }

        }

        return Hazelcast.newHazelcastInstance(config);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Set<ValidationResult> results = new HashSet<>();

        if (!context.getProperty(HAZELCAST_PORT).isSet() && context.getProperty(HAZELCAST_PORT_COUNT).isSet()) {
            results.add(new ValidationResult.Builder()
                .subject(HAZELCAST_PORT_COUNT.getName())
                .valid(false)
                .explanation("Port must be specified for port count!")
                .build());
        }

        if (!context.getProperty(HAZELCAST_PORT).isSet() && context.getProperty(HAZELCAST_PORT_AUTO_INCREMENT).isSet()) {
            results.add(new ValidationResult.Builder()
                .subject(HAZELCAST_PORT_AUTO_INCREMENT.getName())
                .valid(false)
                .explanation("Port must be specified to use port increment!")
                .build());
        }

        return results;
    }
}
