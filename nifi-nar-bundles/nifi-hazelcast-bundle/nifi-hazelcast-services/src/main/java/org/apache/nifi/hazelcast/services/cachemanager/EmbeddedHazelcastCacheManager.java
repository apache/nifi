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
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Tags({"hazelcast", "cache"})
@CapabilityDescription("A service that runs embedded Hazelcast and provides cache instances backed by that." +
        " The server does not ask for authentication, it is recommended to run it within secured network.")
public class EmbeddedHazelcastCacheManager extends IMapBasedHazelcastCacheManager {

    private static final int DEFAULT_HAZELCAST_PORT = 5701;
    private static final String PORT_SEPARATOR = ":";
    private static final String INSTANCE_CREATION_LOG = "Embedded Hazelcast server instance with instance name %s has been created successfully";
    private static final String MEMBER_LIST_LOG = "Hazelcast cluster will be created based on the NiFi cluster with the following members: %s";

    private static final AllowableValue CLUSTER_NONE = new AllowableValue("none", "None", "No high availability or data replication is provided," +
            " every node has access only to the data stored locally.");
    private static final AllowableValue CLUSTER_ALL_NODES = new AllowableValue("all_nodes", "All Nodes", "Creates Hazelcast cluster based on the NiFi cluster:" +
            " It expects every NiFi nodes to have a running Hazelcast instance on the same port as specified in the Hazelcast Port property. No explicit listing of the" +
            " instances is needed.");
    private static final AllowableValue CLUSTER_EXPLICIT = new AllowableValue("explicit", "Explicit", "Works with an explicit list of Hazelcast instances," +
            " creating a cluster using the listed instances. This provides greater control, making it possible to utilize only certain nodes as Hazelcast servers." +
            " The list of Hazelcast instances can be set in the property \"Hazelcast Instances\". The list items must refer to hosts within the NiFi cluster, no external Hazelcast" +
            " is allowed. NiFi nodes are not listed will be join to the Hazelcast cluster as clients.");

    private static final PropertyDescriptor HAZELCAST_PORT = new PropertyDescriptor.Builder()
            .name("hazelcast-port")
            .displayName("Hazelcast Port")
            .description("Port for the Hazelcast instance to use.")
            .required(true)
            .defaultValue(String.valueOf(DEFAULT_HAZELCAST_PORT))
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final PropertyDescriptor HAZELCAST_CLUSTERING_STRATEGY = new PropertyDescriptor.Builder()
            .name("hazelcast-clustering-strategy")
            .displayName("Hazelcast Clustering Strategy")
            .description("Specifies with what strategy the Hazelcast cluster should be created.")
            .required(true)
            .allowableValues(CLUSTER_NONE, CLUSTER_ALL_NODES, CLUSTER_EXPLICIT)
            .defaultValue(CLUSTER_NONE.getValue()) // None is used for default in order to be valid with standalone NiFi.
            .build();

    private static final PropertyDescriptor HAZELCAST_INSTANCES = new PropertyDescriptor.Builder()
            .name("hazelcast-instances")
            .displayName("Hazelcast Instances")
            .description("Only used with \"Explicit\" Clustering Strategy!" +
                    " List of NiFi instance host names which should be part of the Hazelcast cluster. Host names are separated by comma." +
                    " The port specified in the \"Hazelcast Port\" property will be used as server port." +
                    " The list must contain every instance that will be part of the cluster. Other instances will join the Hazelcast cluster as clients.")
            .required(false)
            // HOSTNAME_PORT_LIST_VALIDATOR would not work properly as we do not expect port here, only list of hosts. Custom validator provides further checks.
            .addValidator(StandardValidators.URI_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS;

    static {
        PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
                HAZELCAST_CLUSTER_NAME,
                HAZELCAST_PORT,
                HAZELCAST_CLUSTERING_STRATEGY,
                HAZELCAST_INSTANCES
        ));
    }

    @Override
    protected HazelcastInstance getInstance(final ConfigurationContext context) {
        final String instanceName = UUID.randomUUID().toString();
        final Config config = new Config(instanceName);
        final NetworkConfig networkConfig = config.getNetworkConfig();
        final TcpIpConfig tcpIpConfig = networkConfig.getJoin().getTcpIpConfig();
        final String clusteringStrategy = context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue();
        final String clusterName = context.getProperty(HAZELCAST_CLUSTER_NAME).evaluateAttributeExpressions().getValue();
        final int port = context.getProperty(HAZELCAST_PORT).evaluateAttributeExpressions().asInteger();

        config.setClusterName(clusterName);

        // If clustering is turned off, we turn off the capability of the Hazelcast instance to form a cluster.
        tcpIpConfig.setEnabled(!clusteringStrategy.equals(CLUSTER_NONE.getValue()));

        // Multicasting and automatic port increment are explicitly turned off.
        networkConfig.setPort(port);
        networkConfig.setPortCount(1);
        networkConfig.setPortAutoIncrement(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);

        final HazelcastInstance result;

        if (clusteringStrategy.equals(CLUSTER_ALL_NODES.getValue())) {
            final List<String> hazelcastMembers = getNodeTypeProvider()
                    .getClusterMembers()
                    .stream()
                    .map(m -> m + PORT_SEPARATOR + port)
                    .collect(Collectors.toList());

            getLogger().info(String.format(MEMBER_LIST_LOG, hazelcastMembers.stream().collect(Collectors.joining(", "))));
            tcpIpConfig.setMembers(hazelcastMembers);
            result = Hazelcast.newHazelcastInstance(config);
            getLogger().info(String.format(INSTANCE_CREATION_LOG, instanceName));

        } else if (clusteringStrategy.equals(CLUSTER_EXPLICIT.getValue())) {
            final List<String> hazelcastMembers = getHazelcastMemberHosts(context);

            if (hazelcastMembers.contains(getNodeTypeProvider().getCurrentNode().get())) {
                tcpIpConfig.setMembers(hazelcastMembers.stream().map(m -> m + PORT_SEPARATOR + port).collect(Collectors.toList()));
                result = Hazelcast.newHazelcastInstance(config);
                getLogger().info(String.format(INSTANCE_CREATION_LOG, instanceName));
            } else {
                result = getClientInstance(
                        clusterName,
                        hazelcastMembers.stream().map(m -> m + PORT_SEPARATOR + port).collect(Collectors.toList()),
                        TimeUnit.SECONDS.toMillis(DEFAULT_CLIENT_TIMEOUT_MAXIMUM_IN_SEC),
                        Long.valueOf(TimeUnit.SECONDS.toMillis(DEFAULT_CLIENT_BACKOFF_INITIAL_IN_SEC)).intValue(),
                        Long.valueOf(TimeUnit.SECONDS.toMillis(DEFAULT_CLIENT_BACKOFF_MAXIMUM_IN_SEC)).intValue(),
                        DEFAULT_CLIENT_BACKOFF_MULTIPLIER);
                getLogger().info("This host was not part of the expected Hazelcast instances. Hazelcast client has been started and joined to listed instances.");
            }
        } else if (clusteringStrategy.equals(CLUSTER_NONE.getValue())) {
            result = Hazelcast.newHazelcastInstance(config);
            getLogger().info(String.format(INSTANCE_CREATION_LOG, instanceName));
        } else {
            throw new ProcessException("Unknown Hazelcast Clustering Strategy!");
        }

        return result;
    }

    private List<String> getHazelcastMemberHosts(final PropertyContext context) {
        return Arrays.asList(context.getProperty(HAZELCAST_INSTANCES).evaluateAttributeExpressions().getValue().split(ADDRESS_SEPARATOR))
                .stream().map(i -> i.trim()).collect(Collectors.toList());
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new LinkedList<>();

        // This validation also prevents early activation: up to the point node is considered as clustered this validation step
        // prevents automatic enabling when the flow stars (and the last known status of the controller service was enabled).
        if (!getNodeTypeProvider().isClustered() && context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue().equals(CLUSTER_ALL_NODES.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_CLUSTERING_STRATEGY.getDisplayName())
                    .valid(false)
                    .explanation("cannot use \"" + CLUSTER_ALL_NODES.getDisplayName() + "\" Clustering Strategy when NiFi is not part of a cluster!")
                    .build());
        }

        if (!getNodeTypeProvider().isClustered() && context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue().equals(CLUSTER_EXPLICIT.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_CLUSTERING_STRATEGY.getDisplayName())
                    .valid(false)
                    .explanation("cannot use \"" + CLUSTER_EXPLICIT.getDisplayName() + "\" Clustering Strategy when NiFi is not part of a cluster!")
                    .build());
        }

        if (!context.getProperty(HAZELCAST_INSTANCES).isSet() && context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue().equals(CLUSTER_EXPLICIT.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_INSTANCES.getDisplayName())
                    .valid(false)
                    .explanation("in case of \"" + CLUSTER_EXPLICIT.getDisplayName() + "\" Clustering Strategy, instances need to be specified!")
                    .build());
        }

        if (context.getProperty(HAZELCAST_INSTANCES).isSet() && !context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue().equals(CLUSTER_EXPLICIT.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_INSTANCES.getDisplayName())
                    .valid(false)
                    .explanation("in case of other Clustering Strategy than \"" + CLUSTER_EXPLICIT.getDisplayName() + "\", instances should not be specified!")
                    .build());
        }

        if (context.getProperty(HAZELCAST_INSTANCES).isSet() && context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue().equals(CLUSTER_EXPLICIT.getValue())) {
            final Collection<String> niFiHosts = getNodeTypeProvider().getClusterMembers();
            final Collection<String> hazelcastHosts = getHazelcastMemberHosts(context);

            for (final String hazelcastHost : hazelcastHosts) {
                if (!niFiHosts.contains(hazelcastHost)) {
                    results.add(new ValidationResult.Builder()
                            .subject(HAZELCAST_INSTANCES.getDisplayName())
                            .valid(false)
                            .explanation("host \"" + hazelcastHost + "\" is not part of the NiFi cluster!")
                            .build());
                }
            }
        }

        if (!getNodeTypeProvider().getCurrentNode().isPresent() && context.getProperty(HAZELCAST_CLUSTERING_STRATEGY).getValue().equals(CLUSTER_EXPLICIT.getValue())) {
            results.add(new ValidationResult.Builder()
                    .subject(HAZELCAST_CLUSTERING_STRATEGY.getDisplayName())
                    .valid(false)
                    .explanation("cannot determine current node's host!")
                    .build());
        }

        return results;
    }
}
