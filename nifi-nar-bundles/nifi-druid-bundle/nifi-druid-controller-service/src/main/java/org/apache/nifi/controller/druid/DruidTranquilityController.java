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

package org.apache.nifi.controller.druid;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.AggregatorsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.query.aggregation.datasketches.theta.SketchModule;
import io.druid.query.aggregation.histogram.ApproximateHistogramDruidModule;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.api.druid.DruidTranquilityService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;

import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.DruidBeamConfig;
import com.metamx.tranquility.druid.DruidBeams;
import com.metamx.tranquility.druid.DruidDimensions;
import com.metamx.tranquility.druid.DruidEnvironment;
import com.metamx.tranquility.druid.DruidLocation;
import com.metamx.tranquility.druid.DruidRollup;
import com.metamx.tranquility.tranquilizer.Tranquilizer;
import com.metamx.tranquility.typeclass.Timestamper;

import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.aggregation.AggregatorFactory;
import org.joda.time.DateTime;
import org.joda.time.Period;


@Tags({"Druid", "Timeseries", "OLAP", "ingest"})
@CapabilityDescription("Asynchronously sends flowfiles to Druid Indexing Task using Tranquility API. "
        + "If aggregation and roll-up of data is required, an Aggregator JSON descriptor needs to be provided."
        + "Details on how describe aggregation using JSON can be found at: http://druid.io/docs/latest/querying/aggregations.html")
public class DruidTranquilityController extends AbstractControllerService implements DruidTranquilityService {
    private final static String FIREHOSE_PATTERN = "druid:firehose:%s";

    private final static AllowableValue PT1M = new AllowableValue("PT1M", "1 minute", "1 minute");
    private final static AllowableValue PT10M = new AllowableValue("PT10M", "10 minutes", "10 minutes");
    private final static AllowableValue PT60M = new AllowableValue("PT60M", "60 minutes", "1 hour");

    private final static List<String> TIME_ORDINALS = Arrays.asList("SECOND", "MINUTE", "FIVE_MINUTE", "TEN_MINUTE", "FIFTEEN_MINUTE", "HOUR", "SIX_HOUR", "DAY", "WEEK", "MONTH", "YEAR");

    private Tranquilizer tranquilizer = null;
    private String transitUri = "";

    public static final PropertyDescriptor DATASOURCE = new PropertyDescriptor.Builder()
            .name("druid-cs-data-source")
            .displayName("Druid Data Source")
            .description("A data source is the Druid equivalent of a database table.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor ZOOKEEPER_CONNECTION_STRING = new PropertyDescriptor.Builder()
            .name("druid-cs-zk-connect-string")
            .displayName("Zookeeper Connection String")
            .description("A comma-separated list of host:port pairs, each corresponding to a ZooKeeper server. Ex: localhost:2181")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor ZOOKEEPER_RETRY_BASE_SLEEP_TIME = new PropertyDescriptor.Builder()
            .name("druid-cs-zk-retry-base-sleep")
            .displayName("Zookeeper Retry Base Sleep Time")
            .description("When a connection to Zookeeper needs to be retried, this property specifies the amount of time (in milliseconds) to wait at first before retrying.")
            .required(true)
            .defaultValue("1000")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor ZOOKEEPER_RETRY_MAX_RETRIES = new PropertyDescriptor.Builder()
            .name("druid-cs-zk-retry-max-retries")
            .displayName("Zookeeper Retry Max Retries")
            .description("When a connection to Zookeeper needs to be retried, this property specifies how many times to attempt reconnection.")
            .required(true)
            .defaultValue("20")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor ZOOKEEPER_RETRY_SLEEP_TIME = new PropertyDescriptor.Builder()
            .name("druid-cs-zk-retry-sleep")
            .displayName("Zookeeper Retry Sleep Time")
            .description("When a connection to Zookeeper needs to be retried, this property specifies the amount of time to sleep (in milliseconds) between retries.")
            .required(true)
            .defaultValue("30000")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor DRUID_INDEX_SERVICE_PATH = new PropertyDescriptor.Builder()
            .name("druid-cs-index-service-path")
            .displayName("Index Service Path")
            .description("Druid Index Service path as defined via the Druid Overlord druid.service property.")
            .required(true)
            .defaultValue("druid/overlord")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DRUID_DISCOVERY_PATH = new PropertyDescriptor.Builder()
            .name("druid-cs-discovery-path")
            .displayName("Discovery Path")
            .description("Druid Discovery Path as configured in Druid Common druid.discovery.curator.path property")
            .required(true)
            .defaultValue("/druid/discovery")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor CLUSTER_PARTITIONS = new PropertyDescriptor.Builder()
            .name("druid-cs-cluster-partitions")
            .displayName("Cluster Partitions")
            .description("The number of partitions in the Druid cluster.")
            .required(true)
            .defaultValue("1")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLUSTER_REPLICATION = new PropertyDescriptor.Builder()
            .name("druid-cs-cluster-replication")
            .displayName("Cluster Replication")
            .description("The replication factor for the Druid cluster.")
            .required(true)
            .defaultValue("1")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
            .name("druid-cs-timestamp-field")
            .displayName("Timestamp field")
            .description("The name of the field that will be used as the timestamp. Should be in ISO8601 format.")
            .required(true)
            .defaultValue("timestamp")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor AGGREGATOR_JSON = new PropertyDescriptor.Builder()
            .name("druid-cs-aggregators-descriptor")
            .displayName("Aggregator JSON")
            .description("Tranquility-compliant JSON string that defines what aggregators to apply on ingest."
                    + "Example: "
                    + "["
                    + "{"
                    + "\t\"type\" : \"count\","
                    + "\t\"name\" : \"count\","
                    + "},"
                    + "{"
                    + "\t\"name\" : \"value_sum\","
                    + "\t\"type\" : \"doubleSum\","
                    + "\t\"fieldName\" : \"value\""
                    + "},"
                    + "{"
                    + "\t\"fieldName\" : \"value\","
                    + "\t\"name\" : \"value_min\","
                    + "\t\"type\" : \"doubleMin\""
                    + "},"
                    + "{"
                    + "\t\"type\" : \"doubleMax\","
                    + "\t\"name\" : \"value_max\","
                    + "\t\"fieldName\" : \"value\""
                    + "}"
                    + "]")
            .required(true)
            .addValidator((subject, value, context) -> { // Non-empty and valid JSON validator
                if (value == null || value.isEmpty()) {
                    return new ValidationResult.Builder().subject(subject).input(value).valid(false).explanation(subject + " cannot be empty").build();
                }
                try {
                    DruidTranquilityController.parseJsonString(value);
                    return new ValidationResult.Builder().subject(subject).input(value).valid(true).build();
                } catch (IllegalArgumentException iae) {
                    return new ValidationResult.Builder().subject(subject).input(value).valid(false).explanation(subject + " is not valid Aggregator JSON").build();
                }
            })
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DIMENSIONS_LIST = new PropertyDescriptor.Builder()
            .name("druid-cs-dimensions-list")
            .displayName("Dimension Fields")
            .description("A comma separated list of field names that will be stored as dimensions on ingest.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SEGMENT_GRANULARITY = new PropertyDescriptor.Builder()
            .name("druid-cs-segment-granularity")
            .displayName("Segment Granularity")
            .description("Time unit by which to group and aggregate/rollup events. The value must be at least as large as the value of Query Granularity.")
            .required(true)
            .allowableValues("NONE", "SECOND", "MINUTE", "TEN_MINUTE", "HOUR", "DAY", "MONTH", "YEAR")
            .defaultValue("TEN_MINUTE")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUERY_GRANULARITY = new PropertyDescriptor.Builder()
            .name("druid-cs-query-granularity")
            .displayName("Query Granularity")
            .description("Time unit by which to group and aggregate/rollup events. The value must be less than or equal to the value of Segment Granularity.")
            .required(true)
            .allowableValues("NONE", "SECOND", "MINUTE", "FIFTEEN_MINUTE", "THIRTY_MINUTE", "HOUR", "DAY", "MONTH", "YEAR")
            .defaultValue("MINUTE")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INDEX_RETRY_PERIOD = new PropertyDescriptor.Builder()
            .name("druid-cs-index-retry-period")
            .displayName("Index Retry Period")
            .description("Grace period to allow late arriving events for real time ingest.")
            .required(true)
            .allowableValues(PT1M, PT10M, PT60M)
            .defaultValue(PT10M.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor WINDOW_PERIOD = new PropertyDescriptor.Builder()
            .name("druid-cs-window-period")
            .displayName("Late Event Grace Period")
            .description("Grace period to allow late arriving events for real time ingest.")
            .required(true)
            .allowableValues(PT1M, PT10M, PT60M)
            .defaultValue(PT10M.getValue())
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("druid-cs-batch-size")
            .displayName("Batch Size")
            .description("Maximum number of messages to send at once.")
            .required(true)
            .defaultValue("2000")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_PENDING_BATCHES = new PropertyDescriptor.Builder()
            .name("druid-cs-max-pending-batches")
            .displayName("Max Pending Batches")
            .description("Maximum number of batches that may be in flight before service blocks and waits for one to finish.")
            .required(true)
            .defaultValue("5")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor LINGER_MILLIS = new PropertyDescriptor.Builder()
            .name("druid-cs-linger-millis")
            .displayName("Linger (milliseconds)")
            .description("Wait this long for batches to collect more messages (up to Batch Size) before sending them. "
                    + "Set to zero to disable waiting. "
                    + "Set to -1 to always wait for complete batches before sending. ")
            .required(true)
            .defaultValue("1000")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final List<PropertyDescriptor> properties;

    private volatile CuratorFramework curator;
    private volatile int zkBaseSleepMillis;
    private volatile int zkMaxRetries;
    private volatile int zkSleepMillis;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATASOURCE);
        props.add(ZOOKEEPER_CONNECTION_STRING);
        props.add(ZOOKEEPER_RETRY_BASE_SLEEP_TIME);
        props.add(ZOOKEEPER_RETRY_MAX_RETRIES);
        props.add(ZOOKEEPER_RETRY_SLEEP_TIME);
        props.add(DRUID_INDEX_SERVICE_PATH);
        props.add(DRUID_DISCOVERY_PATH);
        props.add(CLUSTER_PARTITIONS);
        props.add(CLUSTER_REPLICATION);
        props.add(DIMENSIONS_LIST);
        props.add(AGGREGATOR_JSON);
        props.add(SEGMENT_GRANULARITY);
        props.add(QUERY_GRANULARITY);
        props.add(WINDOW_PERIOD);
        props.add(TIMESTAMP_FIELD);
        props.add(MAX_BATCH_SIZE);
        props.add(MAX_PENDING_BATCHES);
        props.add(LINGER_MILLIS);

        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>();
        final String segmentGranularity = validationContext.getProperty(SEGMENT_GRANULARITY).getValue();
        final String queryGranularity = validationContext.getProperty(QUERY_GRANULARITY).getValue();

        // Verify that segment granularity is as least as large as query granularity
        if (TIME_ORDINALS.indexOf(segmentGranularity) < TIME_ORDINALS.indexOf(queryGranularity)) {
            results.add(new ValidationResult.Builder().valid(false).explanation(
                    "Segment Granularity must be at least as large as Query Granularity").build());
        }

        return results;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) {
        ComponentLog log = getLogger();
        log.info("Starting Druid Tranquility Controller Service...");

        final String dataSource = context.getProperty(DATASOURCE).evaluateAttributeExpressions().getValue();
        final String zkConnectString = context.getProperty(ZOOKEEPER_CONNECTION_STRING).evaluateAttributeExpressions().getValue();
        zkBaseSleepMillis = context.getProperty(ZOOKEEPER_RETRY_BASE_SLEEP_TIME).evaluateAttributeExpressions().asInteger();
        zkMaxRetries = context.getProperty(ZOOKEEPER_RETRY_BASE_SLEEP_TIME).evaluateAttributeExpressions().asInteger();
        zkSleepMillis = context.getProperty(ZOOKEEPER_RETRY_SLEEP_TIME).evaluateAttributeExpressions().asInteger();
        final String indexService = context.getProperty(DRUID_INDEX_SERVICE_PATH).evaluateAttributeExpressions().getValue();
        final String discoveryPath = context.getProperty(DRUID_DISCOVERY_PATH).evaluateAttributeExpressions().getValue();
        final int clusterPartitions = context.getProperty(CLUSTER_PARTITIONS).evaluateAttributeExpressions().asInteger();
        final int clusterReplication = context.getProperty(CLUSTER_REPLICATION).evaluateAttributeExpressions().asInteger();
        final String timestampField = context.getProperty(TIMESTAMP_FIELD).evaluateAttributeExpressions().getValue();
        final String segmentGranularity = context.getProperty(SEGMENT_GRANULARITY).getValue();
        final String queryGranularity = context.getProperty(QUERY_GRANULARITY).getValue();
        final String windowPeriod = context.getProperty(WINDOW_PERIOD).getValue();
        final String indexRetryPeriod = context.getProperty(INDEX_RETRY_PERIOD).getValue();
        final String aggregatorJSON = context.getProperty(AGGREGATOR_JSON).evaluateAttributeExpressions().getValue();
        final String dimensionsStringList = context.getProperty(DIMENSIONS_LIST).evaluateAttributeExpressions().getValue();
        final int maxBatchSize = context.getProperty(MAX_BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final int maxPendingBatches = context.getProperty(MAX_PENDING_BATCHES).evaluateAttributeExpressions().asInteger();
        final int lingerMillis = context.getProperty(LINGER_MILLIS).evaluateAttributeExpressions().asInteger();

        transitUri = String.format(FIREHOSE_PATTERN, dataSource) + ";indexServicePath=" + indexService;

        final List<String> dimensions = getDimensions(dimensionsStringList);
        final List<AggregatorFactory> aggregator = getAggregatorList(aggregatorJSON);

        final Timestamper<Map<String, Object>> timestamper = new Timestamper<Map<String, Object>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public DateTime timestamp(Map<String, Object> theMap) {
                return new DateTime(theMap.get(timestampField));
            }
        };

        Iterator<AggregatorFactory> aggIterator = aggregator.iterator();
        AggregatorFactory currFactory;
        log.debug("Number of Aggregations Defined: {}", new Object[]{aggregator.size()});
        while (aggIterator.hasNext()) {
            currFactory = aggIterator.next();
            log.debug("Verifying Aggregator Definition\n\tAggregator Name: {}\n\tAggregator Type: {}\n\tAggregator Req Fields: {}",
                    new Object[]{currFactory.getName(), currFactory.getTypeName(), currFactory.requiredFields()});
        }
        // Tranquility uses ZooKeeper (through Curator) for coordination.
        curator = getCurator(zkConnectString);
        curator.start();

        // The JSON serialization of your object must have a timestamp field in a format that Druid understands. By default,
        // Druid expects the field to be called "timestamp" and to be an ISO8601 timestamp.
        final TimestampSpec timestampSpec = new TimestampSpec(timestampField, "auto", null);

        final Beam<Map<String, Object>> beam = buildBeam(dataSource, indexService, discoveryPath, clusterPartitions, clusterReplication,
                segmentGranularity, queryGranularity, windowPeriod, indexRetryPeriod, dimensions, aggregator, timestamper, timestampSpec);

        tranquilizer = buildTranquilizer(maxBatchSize, maxPendingBatches, lingerMillis, beam);

        tranquilizer.start();
    }

    Tranquilizer<Map<String, Object>> buildTranquilizer(int maxBatchSize, int maxPendingBatches, int lingerMillis, Beam<Map<String, Object>> beam) {
        return Tranquilizer.builder()
                .maxBatchSize(maxBatchSize)
                .maxPendingBatches(maxPendingBatches)
                .lingerMillis(lingerMillis)
                .blockOnFull(true)
                .build(beam);
    }

    Beam<Map<String, Object>> buildBeam(String dataSource, String indexService, String discoveryPath, int clusterPartitions, int clusterReplication,
                                        String segmentGranularity, String queryGranularity, String windowPeriod, String indexRetryPeriod, List<String> dimensions,
                                        List<AggregatorFactory> aggregator, Timestamper<Map<String, Object>> timestamper, TimestampSpec timestampSpec) {
        return DruidBeams.builder(timestamper)
                .curator(curator)
                .discoveryPath(discoveryPath)
                .location(DruidLocation.create(DruidEnvironment.create(indexService, FIREHOSE_PATTERN), dataSource))
                .timestampSpec(timestampSpec)
                .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregator, QueryGranularity.fromString(queryGranularity)))
                .tuning(
                        ClusteredBeamTuning
                                .builder()
                                .segmentGranularity(getGranularity(segmentGranularity))
                                .windowPeriod(new Period(windowPeriod))
                                .partitions(clusterPartitions)
                                .replicants(clusterReplication)
                                .build()
                )
                .druidBeamConfig(
                        DruidBeamConfig
                                .builder()
                                .indexRetryPeriod(new Period(indexRetryPeriod))
                                .build())
                .buildBeam();
    }

    @OnDisabled
    public void onDisabled() {
        if (tranquilizer != null) {
            tranquilizer.flush();
            tranquilizer.stop();
            tranquilizer = null;
        }

        if (curator != null) {
            curator.close();
            curator = null;
        }
    }

    public Tranquilizer getTranquilizer() {
        return tranquilizer;
    }

    CuratorFramework getCurator(String zkConnectString) {
        return CuratorFrameworkFactory
                .builder()
                .connectString(zkConnectString)
                .retryPolicy(new ExponentialBackoffRetry(zkBaseSleepMillis, zkMaxRetries, zkSleepMillis))
                .build();
    }

    @Override
    public String getTransitUri() {
        return transitUri;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, String>> parseJsonString(String aggregatorJson) {
        if (aggregatorJson == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectMapper mapper = new ObjectMapper();
        final List<Map<String, String>> aggSpecList;
        try {
            aggSpecList = mapper.readValue(aggregatorJson, List.class);
            return aggSpecList;
        } catch (IOException e) {
            throw new IllegalArgumentException("Exception while parsing the aggregrator JSON");
        }
    }

    private List<String> getDimensions(String dimensionStringList) {
        List<String> dimensionList = new ArrayList<>();
        if (dimensionStringList != null) {
            Arrays.stream(dimensionStringList.split(","))
                    .filter(StringUtils::isNotBlank)
                    .map(String::trim)
                    .forEach(dimensionList::add);
        }
        return dimensionList;
    }

    private List<AggregatorFactory> getAggregatorList(String aggregatorJSON) {
        ComponentLog log = getLogger();
        ObjectMapper mapper = new ObjectMapper(null);
        mapper.registerModule(new AggregatorsModule());
        mapper.registerModules(Lists.newArrayList(new SketchModule().getJacksonModules()));
        mapper.registerModules(Lists.newArrayList(new ApproximateHistogramDruidModule().getJacksonModules()));

        try {
            return mapper.readValue(
                aggregatorJSON,
                new TypeReference<List<AggregatorFactory>>() {
                }
            );
        } catch (IOException e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    private Granularity getGranularity(String granularityString) {
        Granularity granularity = Granularity.HOUR;

        switch (granularityString) {
            case "SECOND":
                granularity = Granularity.SECOND;
                break;
            case "MINUTE":
                granularity = Granularity.MINUTE;
                break;
            case "FIVE_MINUTE":
                granularity = Granularity.FIVE_MINUTE;
                break;
            case "TEN_MINUTE":
                granularity = Granularity.TEN_MINUTE;
                break;
            case "FIFTEEN_MINUTE":
                granularity = Granularity.FIFTEEN_MINUTE;
                break;
            case "HOUR":
                granularity = Granularity.HOUR;
                break;
            case "SIX_HOUR":
                granularity = Granularity.SIX_HOUR;
                break;
            case "DAY":
                granularity = Granularity.DAY;
                break;
            case "WEEK":
                granularity = Granularity.WEEK;
                break;
            case "MONTH":
                granularity = Granularity.MONTH;
                break;
            case "YEAR":
                granularity = Granularity.YEAR;
                break;
            default:
                break;
        }
        return granularity;
    }
}
