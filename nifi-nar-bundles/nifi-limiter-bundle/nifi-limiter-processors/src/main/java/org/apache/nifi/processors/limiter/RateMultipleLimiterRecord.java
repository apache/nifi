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
package org.apache.nifi.processors.limiter;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.expression.ExpressionLanguageScope;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMapCantBeCreatedOnLiteMemberException;

import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.ConsumptionProbe;
import io.github.bucket4j.Refill;
import io.github.bucket4j.grid.BucketNotFoundException;
import io.github.bucket4j.grid.ProxyManager;
import io.github.bucket4j.grid.hazelcast.Hazelcast;


@Tags({ "record", "rate limiter", "flooding", "record", "multiple configuration" })
@CapabilityDescription("Provide a system to limit the incoming traffic in nifi. Each record will be limited using the provided key" 
        + "that can be defined using a record path. In addition, each record can provide its rate limiter configuration.")
@SeeAlso({ RateLimiter.class })
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({
                @WritesAttribute(attribute = "record.count", description = "The FlowFile will have a 'record.count' attribute indicating the number of records "
                                + "that were written to the FlowFile."),
                @WritesAttribute(attribute = "record.flooding", description = "The FlowFile will have a 'record.flooding' attribute indicating the number of records "
                                + "that were considered provider flooding the system"),
                @WritesAttribute(attribute = "mime.type", description = "The MIME Type indicated by the Record Writer") })
public class RateMultipleLimiterRecord extends AbstractProcessor implements RateLimiterProcessor {
        protected static final String ADDRESS_SEPARATOR = ",";

        static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder().name("put-es-record-reader")
                        .displayName("Record Reader")
                        .description("The record reader to use for reading incoming records from flowfiles.")
                        .identifiesControllerService(RecordReaderFactory.class).required(true).build();

        static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder().name("record-writer")
                        .displayName("Record Writer")
                        .description("The Record Writer to use in order to serialize the data before sending to Kafka")
                        .identifiesControllerService(RecordSetWriterFactory.class)
                        .expressionLanguageSupported(ExpressionLanguageScope.NONE).required(true).build();

        public static final PropertyDescriptor HAZELCAST_SERVER_ADDRESS = new PropertyDescriptor.Builder()
                        .name("hazelcast-server-address").displayName("Hazelcast Server Address")
                        .description("Addresses of one or more the Hazelcast instances, using {host:port} format, separated by comma.")
                        .required(true).defaultValue("localhost:5701")
                        .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
                        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();

        static final PropertyDescriptor HAZELCAST_MAP = new PropertyDescriptor.Builder().name("put-hazelcast-map")
                        .displayName("Hazelcast Map name").description("Name of the IMAP object to query in hazelcast")
                        .required(true).expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

        static final PropertyDescriptor KEY_RECORD_PATH = new PropertyDescriptor.Builder().name("put-key-record-path")
                        .displayName("Key Record Path")
                        .description("A record path expression to retrieve the key field to use with rate limiter. If left blank "
                                        + "the key will be determined using the main key property.")
                        .addValidator(new RecordPathValidator())
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

        static final PropertyDescriptor KEY_REMOVED = new PropertyDescriptor.Builder().name("put-key-removed")
                        .displayName("Key Removed")
                        .description("Decide if the record key path  used for rate limiter should be removed or not"
                                        + "from the records.")
                        .required(true).allowableValues("true", "false").defaultValue("false")
                        .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

        static final PropertyDescriptor REFILL_TOKENS_RECORD_PATH = new PropertyDescriptor.Builder()
                        .name("put-refill-tokens-record-path").displayName("Refill Tokens Record Path")
                        .description("A record path expression to retrieve the refill tokens field to use with rate limiter. If left blank "
                                        + "the refill tokens will be determined using the main refill tokens property.")
                        .addValidator(new RecordPathValidator())
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

        static final PropertyDescriptor REFILL_TOKENS_REMOVED = new PropertyDescriptor.Builder()
                        .name("put-refill-tokens-removed").displayName("Refill Tokens Removed")
                        .description("Decide if the record refill tokens path used for rate limiter should be removed or not"
                                        + "from the records.")
                        .required(true).allowableValues("true", "false").defaultValue("false")
                        .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

        static final PropertyDescriptor REFILL_PERIOD_RECORD_PATH = new PropertyDescriptor.Builder()
                        .name("put-refill-period-record-path").displayName("Refill Period Record Path")
                        .description("A record path expression to retrieve the refill period field to use with rate limiter. If left blank "
                                        + "the refill period will be determined using the main refill period property.")
                        .addValidator(new RecordPathValidator())
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

        static final PropertyDescriptor REFILL_PERIOD_REMOVED = new PropertyDescriptor.Builder()
                        .name("put-refill-period-removed").displayName("Refill Period Removed")
                        .description("Decide if the record refill period path used for rate limiter should be removed or not"
                                        + "from the records.")
                        .required(true).allowableValues("true", "false").defaultValue("false")
                        .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

        static final PropertyDescriptor BANDWIDTH_CAPACITY_RECORD_PATH = new PropertyDescriptor.Builder()
                        .name("put-bandwidth-capacity-record-path").displayName("Bandwidth Capacity Record Path")
                        .description("A record path expression to retrieve the bandwidth capacity field to use with rate limiter. If left blank "
                                        + "the bandwidth capacity will be determined using the main bandwidth capacity property.")
                        .addValidator(new RecordPathValidator())
                        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES).build();

        static final PropertyDescriptor BANDWIDTH_CAPACITY_REMOVED = new PropertyDescriptor.Builder()
                        .name("put-bandwidth-capacity-removed").displayName("Bandwidth Capacity Removed")
                        .description("Decide if the record bandwidth capacity path used for rate limiter should be removed or not"
                                        + "from the records.")
                        .required(true).allowableValues("true", "false").defaultValue("false")
                        .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

        static final List<PropertyDescriptor> DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(RECORD_READER,
                        RECORD_WRITER, HAZELCAST_SERVER_ADDRESS, HAZELCAST_MAP, KEY, KEY_RECORD_PATH, KEY_REMOVED,
                        REFILL_TOKENS, REFILL_TOKENS_RECORD_PATH, REFILL_TOKENS_REMOVED, REFILL_PERIOD,
                        REFILL_PERIOD_RECORD_PATH, REFILL_PERIOD_REMOVED, BANDWIDTH_CAPACITY,
                        BANDWIDTH_CAPACITY_RECORD_PATH, BANDWIDTH_CAPACITY_REMOVED, MAX_SIZE_BUCKET, EXPIRE_DURATION));

        static final Set<Relationship> RELATIONSHIPS = Collections
                        .unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE, REL_FLOODING)));

        @Override
        public Set<Relationship> getRelationships() {
                return RELATIONSHIPS;
        }

        @Override
        public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
                return DESCRIPTORS;
        }

        private RecordReaderFactory readerFactory;
        private RecordPathCache recordPathCache;
        private RecordSetWriterFactory writerFactory;
        private HazelcastInstance hazelcastInstance;
        private ProxyManager<String> buckets;

        @OnScheduled
        public void onScheduled(final ProcessContext context) {
                this.readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
                this.writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
                this.recordPathCache = new RecordPathCache(16);

                try {
                        Config config = new Config();
                        String mapName = context.getProperty(HAZELCAST_MAP).getValue(); 
                        //EvictionConfig evictionConfig = new EvictionConfig()
                                //.setEvictionPolicy(EvictionPolicy.LRU)
                                //.setSize(Integer.parseInt(context.getProperty(MAX_SIZE_BUCKET).getValue()));
                        MapConfig mapConfig  = new MapConfig()
                                .setTimeToLiveSeconds(context.getProperty(EXPIRE_DURATION).asTimePeriod(TimeUnit.SECONDS).intValue())
                                //.setEvictionConfig(evictionConfig)
                                .setName(mapName); 
                        NetworkConfig network = config.getNetworkConfig();
                        JoinConfig join = network.getJoin();
                        join.getMulticastConfig().setEnabled( false );
                        join.getTcpIpConfig().setMembers(Arrays.asList(context.getProperty(HAZELCAST_SERVER_ADDRESS).evaluateAttributeExpressions().getValue().split(ADDRESS_SEPARATOR)));
                        config.setProperty("hazelcast.client.statistics.enabled", "true"); 
                        config.addMapConfig(mapConfig); 
                        this.hazelcastInstance = com.hazelcast.core.Hazelcast.newHazelcastInstance(config);
                        this.buckets = Bucket4j.extension(Hazelcast.class).proxyManagerForMap(hazelcastInstance.getMap(mapName));
                } catch (ReplicatedMapCantBeCreatedOnLiteMemberException  exMap) {
                        getLogger().error("This map name already exist on hazelcast server: ", exMap);
                } catch (IllegalStateException exConnection){
                        getLogger().error("The connection to Hazelcast failed: ", exConnection);
                }
                catch (Exception e) {
                        getLogger().error("Could not intiate InternalCache", e);
                }
        }

        @OnUnscheduled
        public void onUnScheduled(final ProcessContext context) {
                if (hazelcastInstance != null) {
                        this.hazelcastInstance.shutdown();
                }
        }

        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
                FlowFile input = session.get();
                if (input == null) {
                        return;
                }

                final String key = context.getProperty(KEY).evaluateAttributeExpressions(input).getValue();

                final String keyPath = context.getProperty(KEY_RECORD_PATH).isSet()
                                ? context.getProperty(KEY_RECORD_PATH).evaluateAttributeExpressions(input).getValue()
                                : null;

                RecordPath _keyPath = keyPath != null ? recordPathCache.getCompiled(keyPath) : null;

                FlowFile output = session.create(input);
                FlowFile flooding = session.create(input);
                FlowFile failure = session.create(input);

                try (InputStream inStream = session.read(input);
                                OutputStream os = session.write(output);
                                OutputStream osFail = session.write(failure);
                                OutputStream osFlooding = session.write(flooding)) {

                        RecordReader reader = readerFactory.createRecordReader(input, inStream, getLogger());
                        RecordSchema schema = writerFactory.getSchema(input.getAttributes(), reader.getSchema());
                        RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, os);
                        RecordSetWriter failWriter = writerFactory.createWriter(getLogger(), schema, osFail);
                        RecordSetWriter floodingWriter = writerFactory.createWriter(getLogger(), schema, osFlooding);
                        Record record;
                        writer.beginRecordSet();
                        failWriter.beginRecordSet();
                        floodingWriter.beginRecordSet();

                        int count = 0;
                        int floodingCount = 0;
                        int failCount = 0;
                        while ((record = reader.nextRecord()) != null) {
                                final String bucketKey = getFromRecordPath(record, _keyPath, key,
                                                context.getProperty(KEY_REMOVED).asBoolean());

                                Bucket requestBucket = null;  
                                Optional<BucketConfiguration> bucketConfig = this.buckets.getProxyConfiguration(bucketKey); 
                                if (bucketConfig.isPresent()) {
                                   requestBucket = this.buckets.getProxy(bucketKey,bucketConfig.get());
                                } else {
                                   requestBucket = this.buckets.getProxy(bucketKey,getConfigSupplier(bucketKey, context, input, record));
                                }
                                

                                if (requestBucket != null) {
                                        // try to consume
                                        ConsumptionProbe probe = requestBucket.tryConsumeAndReturnRemaining(1);

                                        if (probe.isConsumed()) {
                                                // RateLimiterRes: "OK"
                                                count++;
                                                writer.write(record);
                                        } else {
                                                // RateLimiterRes: "KO"
                                                floodingCount++;
                                                floodingWriter.write(record);
                                        }
                                } else {
                                        // RateLimiterRes: "Mybucket Null"
                                        getLogger().error("Rate Limiter : Mybucket Null");
                                        failCount++;
                                        failWriter.write(record);
                                }
                        }

                        writer.finishRecordSet();
                        writer.close();
                        failWriter.finishRecordSet();
                        failWriter.close();
                        floodingWriter.finishRecordSet();
                        floodingWriter.close();
                        inStream.close();
                        os.close();
                        osFail.close();
                        osFlooding.close();

                        if (count > 0) {
                                output = session.putAttribute(output, ATTR_MIME_TYPE, "application/json");
                                output = session.putAttribute(output, ATTR_RECORD_COUNT, String.valueOf(count));
                                output = session.putAttribute(output, ATTR_RECORD_COUNT_FLOODING,
                                                String.valueOf(floodingCount));
                                session.transfer(output, REL_SUCCESS);
                        } else {
                                session.remove(output);
                        }
                        if (floodingCount > 0) {
                                flooding = session.putAttribute(flooding, ATTR_MIME_TYPE, "application/json");
                                flooding = session.putAttribute(flooding, ATTR_RECORD_COUNT,
                                                String.valueOf(floodingCount));
                                session.transfer(flooding, REL_FLOODING);
                        } else {

                                session.remove(flooding);
                        }
                        if (failCount > 0) {
                                failure = session.putAttribute(failure, ATTR_MIME_TYPE, "application/json");
                                failure = session.putAttribute(failure, ATTR_RECORD_COUNT, String.valueOf(failCount));
                                session.transfer(failure, REL_FAILURE);
                        } else {
                                session.remove(failure);
                        }
                        session.remove(input);

                } catch (IOException exIO) {
                        getLogger().error("Failed to consume bucket: ", exIO);
                        session.transfer(input, REL_FAILURE);
                        return;  
                } catch (BucketNotFoundException exBucketNotFound) {
                        getLogger().error("Failed to find bucket, the bucket state has been lost: ", exBucketNotFound);
                        session.transfer(input, REL_FAILURE);
                        return;  
                } catch (IllegalArgumentException exIllArg) {
                        getLogger().error("Failed to build bucket configuration: ", exIllArg);
                        session.transfer(input, REL_FAILURE);
                        return;  
                } catch (Exception ex) {
                        getLogger().error("Could not check internal cache.", ex);
                        session.transfer(input, REL_FAILURE);
                        return;  
                } 
        }

        private String getFromRecordPath(Record record, RecordPath path, final String fallback, Boolean remove) {
                if (path == null) {
                        return fallback;
                }

                RecordPathResult result = path.evaluate(record);
                Optional<FieldValue> value = result.getSelectedFields().findFirst();
                if (value.isPresent()) {
                        FieldValue fieldValue = value.get();
                        Object val = fieldValue.getValue();
                        if (val == null) {
                                return fallback;
                        }
                        if (remove) {
                                fieldValue.updateValue(null);
                        }
                        String realValue = val.toString();

                        return realValue;
                } else {
                        return fallback;
                }
        }

        private Supplier<BucketConfiguration> getConfigSupplier(String apiKey, ProcessContext context, FlowFile input,
                        Record record) throws IllegalArgumentException  {
                return () -> {
                        /* get default values from configuration in processor */
                        final String refillTokens = context.getProperty(REFILL_TOKENS)
                                        .evaluateAttributeExpressions(input).getValue();
                        final String refillPeriod = context.getProperty(REFILL_PERIOD)
                                        .evaluateAttributeExpressions(input).getValue();
                        final String bandwidth = context.getProperty(BANDWIDTH_CAPACITY)
                                        .evaluateAttributeExpressions(input).getValue();

                        /* path to get value in record */
                        final String refillTokensPath = context.getProperty(REFILL_TOKENS_RECORD_PATH).isSet()
                                        ? context.getProperty(REFILL_TOKENS_RECORD_PATH)
                                                        .evaluateAttributeExpressions(input).getValue()
                                        : null;
                        final String refillPeriodPath = context.getProperty(REFILL_PERIOD_RECORD_PATH).isSet()
                                        ? context.getProperty(REFILL_PERIOD_RECORD_PATH)
                                                        .evaluateAttributeExpressions(input).getValue()
                                        : null;
                        final String bandwidthPath = context.getProperty(BANDWIDTH_CAPACITY_RECORD_PATH).isSet()
                                        ? context.getProperty(BANDWIDTH_CAPACITY_RECORD_PATH)
                                                        .evaluateAttributeExpressions(input).getValue()
                                        : null;

                        RecordPath _refillTokensPath = refillTokensPath != null
                                        ? recordPathCache.getCompiled(refillTokensPath)
                                        : null;
                        RecordPath _refillPeriodPath = refillPeriodPath != null
                                        ? recordPathCache.getCompiled(refillPeriodPath)
                                        : null;
                        RecordPath _bandwidthPath = bandwidthPath != null ? recordPathCache.getCompiled(bandwidthPath)
                                        : null;

                        /* get value in record if exist otherwise take default */
                        final String bucketRefillTokens = getFromRecordPath(record, _refillTokensPath, refillTokens,
                                        context.getProperty(REFILL_TOKENS_REMOVED).asBoolean());
                        final String bucketRefillPeriod = getFromRecordPath(record, _refillPeriodPath, refillPeriod,
                                        context.getProperty(REFILL_PERIOD_REMOVED).asBoolean());
                        final String bucketBandwidth = getFromRecordPath(record, _bandwidthPath, bandwidth,
                                        context.getProperty(BANDWIDTH_CAPACITY_REMOVED).asBoolean());

                        /* build bucket4j config && check types are correct */
                        try {
                                Refill refill = Refill.intervally(Integer.parseInt(bucketRefillTokens),
                                                Duration.ofMillis(FormatUtils.getTimeDuration(bucketRefillPeriod,
                                                                TimeUnit.MILLISECONDS)));
                                Bandwidth limit = Bandwidth.classic(Integer.parseInt(bucketBandwidth), refill);
                                return Bucket4j.configurationBuilder().addLimit(limit).build();
                        } catch (IllegalArgumentException  e) {
                                throw new IllegalArgumentException("Building bucket configuration failed (types used are incorrect)");
                        }

                };
        }

}
