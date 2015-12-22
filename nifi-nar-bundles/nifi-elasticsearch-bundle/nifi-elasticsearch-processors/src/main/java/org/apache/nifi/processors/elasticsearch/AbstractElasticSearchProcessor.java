package org.apache.nifi.processors.elasticsearch;

import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import com.google.gson.*;

public abstract class AbstractElasticSearchProcessor extends AbstractProcessor{

    protected static final PropertyDescriptor CLUSTER_NAME = new PropertyDescriptor.Builder()
            .name("Cluster Name")
            .description("Name of the ES cluster. For example, elasticsearch_brew")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    protected static final PropertyDescriptor HOSTS = new PropertyDescriptor.Builder()
            .name("ElasticSearch Hosts")
            .description("ElasticSearch Hosts, which should be comma separated and colon for hostname/port " +
                    "host1:port,host2:port,....  For example testcluster:9300")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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

    protected static final PropertyDescriptor INDEX_STRATEGY = new PropertyDescriptor.Builder()
            .name("Index Strategy")
            .description("Pick the index strategy. Yearly, Monthly, Daily, Hourly")
            .required(true)
            .defaultValue("Monthly")
            .allowableValues("Yearly", "Monthly", "Daily", "Hourly")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected TransportClient esClient;
    protected List<InetSocketAddress> esHosts;
    protected String indexPrefix;
    protected static String indexStrategy;

    /**
     * Instantiate ElasticSearch Client
     * @param context
     * @throws IOException
     */
    @OnScheduled
    public final void createClient(ProcessContext context) throws IOException {
        if (esClient != null) {
            closeClient();
        }

        getLogger().info("Creating ElasticSearch Client");

        try {

            final String clusterName = context.getProperty(CLUSTER_NAME).toString();
            final String pingTimeout = context.getProperty(PING_TIMEOUT).toString();
            final String samplerInterval = context.getProperty(SAMPLER_INTERVAL).toString();
            indexStrategy = context.getProperty(INDEX_STRATEGY).toString();

            //create new transport client
            esClient = new TransportClient(
                    ImmutableSettings.builder()
                            .put("cluster.name", clusterName)
                            .put("client.transport.ping_timeout", pingTimeout)
                            .put("client.transport.nodes_sampler_interval", samplerInterval),
                    false);


            final String hosts = context.getProperty(HOSTS).toString();
            esHosts = GetEsHosts(hosts);

            for (final InetSocketAddress host : esHosts) {
                esClient.addTransportAddress(new InetSocketTransportAddress(host));
            }
        } catch (Exception e) {
            getLogger().error("Failed to schedule PutElasticSearch due to {}", new Object[] { e }, e);
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
     * Get the ElasticSearch hosts from the Nifi attribute
     * @param hosts A comma separted list of ElasticSearch hosts
     * @return List of InetSockeAddresses for the ES hosts
     */
    private List<InetSocketAddress> GetEsHosts(String hosts){

        final List<String> esList = Arrays.asList(hosts.split(","));
        List<InetSocketAddress> esHosts = new ArrayList<>();

        for(String item : esList){

            String[] addresses = item.split(":");
            final String hostName = addresses[0];
            final int port = Integer.parseInt(addresses[1]);

            esHosts.add(new InetSocketAddress(hostName, port));
        }

        return esHosts;

    }


    /**
     * Get ElasticSearch index for data
     * @param input
     * @return
     */
    public String getIndex(final JsonObject input) {

        return extractIndexString(input);
    }

    /**
     * Get ElasticSearch Type
     * @param input
     * @return
     */
    public String getType(final JsonObject input) {
        return "status";
    }

    /**
     * Get id for ElasticSearch
     * @param input
     * @return
     */
    public String getId(final JsonObject input) {

        return input.get("id").getAsString();
    }

    /**
     * Get Source for ElasticSearch
     * @param input
     * @return
     */
    public byte[] getSource(final JsonObject input) {
        String jsonString = input.toString();
        jsonString = jsonString.replace("\r\n", " ").replace('\n', ' ').replace('\r', ' ');
        return jsonString.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Identify ElasticSearch index where data will land
     * @param parsedJson
     * @return
     */
    private static String extractIndexString(final JsonObject parsedJson) {
        final String extractedDate = "created_at";
        if(!parsedJson.has(extractedDate))
            throw new IllegalArgumentException("Message is missing " + extractedDate);

        final DateTimeFormatter format =
                DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withLocale(Locale.ENGLISH);

        final String dateElement = parsedJson.get(extractedDate).getAsString();
        final DateTimeFormatter isoFormat = ISODateTimeFormat.dateTime();
        final DateTime dateTime = isoFormat.parseDateTime(format.parseDateTime(dateElement).toString());

        final DateTimeFormatter dateFormat;
        //Create ElasticSearch Index
        switch (indexStrategy){

            case "Yearly":
                dateFormat = DateTimeFormat.forPattern("yyyy_MM");
                break;
            case "Monthly":
                dateFormat = DateTimeFormat.forPattern("yyyy_MM");
                break;
            case "Daily":
                dateFormat = DateTimeFormat.forPattern("yyyy_MM_dd");
                break;
            case "Hourly":
                dateFormat = DateTimeFormat.forPattern("yyyy_MM_dd_HH");
                break;
            default:
                throw new IllegalArgumentException("Invalid index strategy selected: " + indexStrategy);

        }

        //ElasticSearch indexes must be lowercase
        final String strategy = indexStrategy.toLowerCase() + "_" + dateFormat.print(dateTime);
        return strategy;
    }
}
