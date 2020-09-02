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
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Tags({"hazelcast", "cache"})
@CapabilityDescription("A service that provides connections to an embedded Hazelcast instance started by NiFi." +
        " The server does not asks for authentication, it is suggested to run it within secured network.")
public class EmbeddedHazelcastCacheManager extends IMapBasedHazelcastCacheManager {

    private static final int DEFAULT_HAZELCAST_PORT = 5701;

    private static final AllowableValue HA_NONE = new AllowableValue("none", "None", "No high availability or data replication is provided," +
            " every node has access only the data stored within that node.");
    private static final AllowableValue HA_CLUSTER = new AllowableValue("cluster", "Cluster", "Creates Hazelcast cluster based on the NiFi cluster:" +
            " It expects every NiFi nodes to have a running Hazelcast instance on the same port as specified in the Hazelcast Port property. No explicit listing of the" +
            " instances is needed.");
    private static final AllowableValue HA_EXPLICIT = new AllowableValue("explicit", "Explicit", "Works with an explicit list of Hazelcast instances," +
            " creating cluster using those. This provides greater control over the used servers, making it possible to utilize only a number of nodes as Hazelcast server." +
            " The list of Hazelcast instances takes place in property \"Hazelcast Instances\".");

    private static final PropertyDescriptor HAZELCAST_PORT = new PropertyDescriptor.Builder()
            .name("hazelcast-port")
            .displayName("Hazelcast Port")
            .description("Port the Hazelcast uses as starting port. If not specified, the default value will be used, which is " + DEFAULT_HAZELCAST_PORT + ".")
            .required(false)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final PropertyDescriptor HAZELCAST_HA_MODE = new PropertyDescriptor.Builder()
            .name("hazelcast-ha-mode")
            .displayName("Hazelcast High Availability Mode")
            .description("Specifies in what strategy the Hazelcast cluster should be created.")
            .required(true)
            .allowableValues(HA_NONE, HA_CLUSTER, HA_EXPLICIT)
            .defaultValue(HA_NONE.getValue()) // None is used for default in order to be valid with standalone NiFi.
            .build();

    private static final PropertyDescriptor HAZELCAST_INSTANCES = new PropertyDescriptor.Builder()
            .name("hazelcast-instances")
            .displayName("Hazelcast Instances")
            .description("List of Hazelcast instances should be part of the cluster, using {host:port} format separated the instances by comma." +
                    " Only used when high availability mode is set to \"Explicit\". The list must contain every instances will be part of the cluster.")
            .required(false)
            .addValidator(StandardValidators.URI_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    static {
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
                HAZELCAST_CLUSTER_NAME,
                HAZELCAST_PORT,
                HAZELCAST_HA_MODE,
                HAZELCAST_INSTANCES
        ));
    }

    @Override
    protected HazelcastInstance getInstance(final ConfigurationContext context) {
        final String instanceName = UUID.randomUUID().toString();
        final Config config = new Config(instanceName);
        final NetworkConfig networkConfig = config.getNetworkConfig();
        final TcpIpConfig tcpIpConfig = networkConfig.getJoin().getTcpIpConfig();
        final String haMode = context.getProperty(HAZELCAST_HA_MODE).getValue();

        if (context.getProperty(HAZELCAST_CLUSTER_NAME).isSet()) {
            config.setClusterName(context.getProperty(HAZELCAST_CLUSTER_NAME).evaluateAttributeExpressions().getValue());
        }

        final int port = context.getProperty(HAZELCAST_PORT).isSet()
                ? context.getProperty(HAZELCAST_PORT).evaluateAttributeExpressions().asInteger()
                : DEFAULT_HAZELCAST_PORT;

        // It high availability is turned off, we turn of the capability for the Hazelcast instance to form a cluster.
        tcpIpConfig.setEnabled(!haMode.equals(HA_NONE.getValue()));

        // Multicasting and automatic port increment are explicitly turned off.
        networkConfig.setPort(port);
        networkConfig.setPortCount(1);
        networkConfig.setPortAutoIncrement(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);

        if (haMode.equals(HA_CLUSTER.getValue())) {
            final Set<String> members = getNodeTypeProvider().getClusterMembers().stream().map(m -> m + ":" + port).collect(Collectors.toSet());
            getLogger().info("Hazelcast cluster will be created based on the NiFi cluster with the following members: " + members.stream().collect(Collectors.joining(", ")));
            members.forEach(m -> tcpIpConfig.addMember(m));
        } else if (haMode.equals(HA_EXPLICIT.getValue())) {
            final String instances = context.getProperty(HAZELCAST_INSTANCES).evaluateAttributeExpressions().getValue();
            Arrays.asList(instances.split(ADDRESS_SEPARATOR)).stream().map(i -> i.trim()).forEach(node -> tcpIpConfig.addMember(node));
        }

        final HazelcastInstance result = Hazelcast.newHazelcastInstance(config);
        getLogger().info("Embedded Hazelcast server instance with instance name " + instanceName + " has been created successfully");
        return result;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Set<ValidationResult> results = new HashSet<>();

        // This validation also prevents early activation: up to the point node is considered as clustered this validation step
        // prevents automatic enabling when the flow stars (and the last known status of the controller service was enabled).
        if (!getNodeTypeProvider().isClustered() && context.getProperty(HAZELCAST_HA_MODE).getValue().equals(HA_CLUSTER.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_HA_MODE.getName())
                    .valid(false)
                    .explanation("Cannot use Cluster HA mode when NiFi is not part of a cluster!")
                    .build());
        }

        if (!context.getProperty(HAZELCAST_INSTANCES).isSet() && context.getProperty(HAZELCAST_HA_MODE).getValue().equals(HA_EXPLICIT.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_INSTANCES.getName())
                    .valid(false)
                    .explanation("In case of Explicit HA mode, instances needs to be specified!")
                    .build());
        }

        if (context.getProperty(HAZELCAST_INSTANCES).isSet() && !context.getProperty(HAZELCAST_HA_MODE).getValue().equals(HA_EXPLICIT.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_INSTANCES.getName())
                    .valid(false)
                    .explanation("In case of other modes than Explicit HA mode, instances should not be specified!")
                    .build());
        }

        return results;
    }
}
