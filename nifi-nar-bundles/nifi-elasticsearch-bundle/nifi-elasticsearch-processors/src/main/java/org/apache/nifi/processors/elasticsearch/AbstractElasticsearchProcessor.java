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
package org.apache.nifi.processors.elasticsearch;

import com.google.gson.JsonObject;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.node.NodeBuilder;

public abstract class AbstractElasticsearchProcessor extends AbstractProcessor {

    protected static final AllowableValue TRANSPORT_CLIENT =
            new AllowableValue("transport", "Transport",
                    "Specifies a Transport Client be used to connect to the Elasticsearch cluster. A Transport "
                            + "client does not join the cluster, and is better for a large number of connections "
                            + "and/or if the NiFi node(s) and Elasticsearch nodes are mostly isolated via firewall.");

    protected static final AllowableValue NODE_CLIENT =
            new AllowableValue("node", "Node",
                    "Specifies a Node Client be used to connect to the Elasticsearch cluster. This client joins the "
                            + "cluster, so operations are performed more quickly, but the NiFi node may need to be "
                            + "configured such that it can successfully join the Elasticsearch cluster");

    protected static final PropertyDescriptor CLIENT_TYPE = new PropertyDescriptor.Builder()
            .name("Client type")
            .description("The type of client used to connect to the Elasticsearch cluster. Transport client is more "
                    + "isolated and lighter-weight, and Node client is faster and more integrated into the ES cluster")
            .required(true)
            .allowableValues(TRANSPORT_CLIENT, NODE_CLIENT)
            .defaultValue(TRANSPORT_CLIENT.getValue())
            .addValidator(Validator.VALID)
            .build();

    protected static final PropertyDescriptor CLUSTER_NAME = new PropertyDescriptor.Builder()
            .name("Cluster Name")
            .description("Name of the ES cluster (for example, elasticsearch_brew). Defaults to 'elasticsearch'")
            .required(false)
            .addValidator(Validator.VALID)
            .build();
    protected static final PropertyDescriptor HOSTS = new PropertyDescriptor.Builder()
            .name("ElasticSearch Hosts")
            .description("ElasticSearch Hosts, which should be comma separated and colon for hostname/port "
                    + "host1:port,host2:port,....  For example testcluster:9300. Note that this property is only "
                    + "needed when using a Transport client, it is ignored when using a Node client")
            .required(false)
            .addValidator(new ElasticsearchClientValidator())
            .build();
    protected static final PropertyDescriptor PING_TIMEOUT = new PropertyDescriptor.Builder()
            .name("ElasticSearch Ping Timeout")
            .description("The ping timeout used to determine when a node is unreachable.  " +
                    "For example, 5s (5 seconds). If non-local recommended is 30s")
            .required(true)
            .defaultValue("5s")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor SAMPLER_INTERVAL = new PropertyDescriptor.Builder()
            .name("Sampler Interval")
            .description("Node sampler interval. For example, 5s (5 seconds) If non-local recommended is 30s")
            .required(true)
            .defaultValue("5s")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected Client esClient;
    protected List<InetSocketAddress> esHosts;

    /**
     * Instantiate ElasticSearch Client
     *
     * @param context
     * @throws IOException
     */
    @OnScheduled
    public void createClient(ProcessContext context) throws IOException {

        ProcessorLog log = getLogger();
        if (esClient != null) {
            closeClient();
        }

        log.info("Creating ElasticSearch Client");

        try {
            final String clusterType = context.getProperty(CLIENT_TYPE).toString();
            final String clusterName = context.getProperty(CLUSTER_NAME).toString();
            final String pingTimeout = context.getProperty(PING_TIMEOUT).toString();
            final String samplerInterval = context.getProperty(SAMPLER_INTERVAL).toString();

            if ("transport".equals(clusterType)) {

                //create new transport client
                Settings settings = Settings.settingsBuilder()
                        .put("cluster.name", clusterName)
                        .put("client.transport.ping_timeout", pingTimeout)
                        .put("client.transport.nodes_sampler_interval", samplerInterval)
                        .build();

                TransportClient transportClient = TransportClient.builder().settings(settings).build();

                final String hosts = context.getProperty(HOSTS).toString();
                esHosts = GetEsHosts(hosts);

                if (esHosts != null) {
                    for (final InetSocketAddress host : esHosts) {
                        transportClient.addTransportAddress(new InetSocketTransportAddress(host));
                    }
                }
                esClient = transportClient;
            } else if ("node".equals(clusterType)) {
                esClient = NodeBuilder.nodeBuilder().clusterName(clusterName).node().client();
            }
        } catch (Exception e) {
            log.error("Failed to create Elasticsearch client due to {}", new Object[]{e}, e);
            throw e;
        }
    }

    /**
     * Dispose of ElasticSearch client
     */
    @OnStopped
    public final void closeClient() {
        if (esClient != null) {
            getLogger().info("Closing ElasticSearch Client");
            esClient.close();
            esClient = null;
        }
    }

    /**
     * Get the ElasticSearch hosts from a Nifi attribute, e.g.
     *
     * @param hosts A comma-separated list of ElasticSearch hosts (host:port,host2:port2, etc.)
     * @return List of InetSocketAddresses for the ES hosts
     */
    private List<InetSocketAddress> GetEsHosts(String hosts) {

        if (hosts == null) {
            return null;
        }
        final List<String> esList = Arrays.asList(hosts.split(","));
        List<InetSocketAddress> esHosts = new ArrayList<>();

        for (String item : esList) {

            String[] addresses = item.split(":");
            final String hostName = addresses[0];
            final int port = Integer.parseInt(addresses[1]);

            esHosts.add(new InetSocketAddress(hostName, port));
        }

        return esHosts;

    }

    /**
     * Get Source for ElasticSearch. The string representation of the JSON object is returned as a byte array after
     * replacing newlines with spaces
     *
     * @param input a JSON object to be serialized to UTF-8
     * @return a byte array containing the UTF-8 representation (without newlines) of the JSON object
     */
    public byte[] getSource(final JsonObject input) {
        String jsonString = input.toString();
        jsonString = jsonString.replace("\r\n", " ").replace('\n', ' ').replace('\r', ' ');
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * A custom validator for the Elasticsearch properties list. For example, the hostnames property doesn't need to
     * be filled in for a Node client, as it joins the cluster by name. Alternatively if a Transport client
     */
    protected static class ElasticsearchClientValidator implements Validator {

        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            // Only validate hosts if cluster type == Transport
            if (HOSTS.getName().equals(subject)) {
                PropertyValue clientTypeProperty = context.getProperty(CLIENT_TYPE);
                if (TRANSPORT_CLIENT.getValue().equals(clientTypeProperty.getValue())) {
                    return StandardValidators.NON_EMPTY_VALIDATOR.validate(
                            CLIENT_TYPE.getName(), clientTypeProperty.getValue(), context);
                }
            }
            return VALID.validate(subject, input, context);
        }
    }

}
