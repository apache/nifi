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
package org.apache.nifi.processors.standard;

import com.google.common.base.Joiner;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.*;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.distributed.cache.client.exception.DeserializationException;
import org.apache.nifi.distributed.cache.client.exception.SerializationException;
import org.apache.nifi.expression.AttributeExpression.ResultType;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathPropertyNameValidator;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.*;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.codec.binary.StringUtils.getBytesUtf8;
import static org.apache.commons.lang3.StringUtils.*;

@EventDriven
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@SystemResourceConsideration(resource = SystemResource.MEMORY,
        description = "Caches records from each incoming FlowFile and determines if the cached record has " +
                "already been seen. The name of user-defined properties determines the RecordPath values used to " +
                "determine if a record is unique. If no user-defined properties are present, the entire record is " +
                "used as the input to determine uniqueness. All duplicate records are routed to 'duplicate'. " +
                "If the record is not determined to be a duplicate, the Processor routes the record to 'non-duplicate'.")
@Tags({"text", "record", "update", "change", "replace", "modify", "distinct", "unique",
        "filter", "hash", "dupe", "duplicate", "dedupe"})
@CapabilityDescription("Caches records from each incoming FlowFile and determines if the cached record has " +
        "already been seen. The name of user-defined properties determines the RecordPath values used to " +
        "determine if a record is unique. If no user-defined properties are present, the entire record is " +
        "used as the input to determine uniqueness. All duplicate records are routed to 'duplicate'. " +
        "If the record is not determined to be a duplicate, the Processor routes the record to 'non-duplicate'."
)
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({
        @WritesAttribute(attribute = "record.count", description = "The number of records in the FlowFile")
})
@DynamicProperty(
        name = "RecordPath",
        value = "User-defined property values are ignored",
        description = "The name of each user-defined property must be a valid RecordPath.")
@SeeAlso(classNames = {
        "org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService",
        "org.apache.nifi.distributed.cache.server.map.DistributedMapCacheServer",
        "org.apache.nifi.processors.standard.DetectDuplicate"
})
public class DetectDuplicateRecord extends AbstractProcessor {

    private volatile RecordPathCache recordPathCache;
    private volatile List<String> recordPaths;

    // VALUES

    static final AllowableValue MD5_ALGORITHM_VALUE = new AllowableValue(MessageDigestAlgorithms.MD5, "MD5",
            "The MD5 message-digest algorithm.");
    static final AllowableValue SHA1_ALGORITHM_VALUE = new AllowableValue(MessageDigestAlgorithms.SHA_1, "SHA-1",
            "The SHA-1 cryptographic hash algorithm.");
    static final AllowableValue SHA256_ALGORITHM_VALUE = new AllowableValue(MessageDigestAlgorithms.SHA3_256, "SHA-256",
            "The SHA-256 cryptographic hash algorithm.");
    static final AllowableValue SHA512_ALGORITHM_VALUE = new AllowableValue(MessageDigestAlgorithms.SHA3_512, "SHA-512",
            "The SHA-512 cryptographic hash algorithm.");

//    static final AllowableValue ENTIRE_RECORD_VALUE = new AllowableValue("entire-record", "Entire Record",
//            "All field values of a record are used in the order they are listed in the incoming FlowFile to determine if two records are equal.");
//    static final AllowableValue RECORD_PATH_VALUE = new AllowableValue("entire-record", "Entire Record",
//            "The user-defined RecordPath properties are used in the specified order to determine whether two records are equal. " +
//                    "If the value of a RecordPath is modified or the ordering of the user-defined RecordPath properties change " +
//                    "after processing one or more FlowFiles, duplicate detection is effectively reset.");

    static final AllowableValue HASH_SET_VALUE = new AllowableValue("hash-set", "HashSet",
            "Exactly matches records seen before with 100% accuracy at the expense of more memory usage. " +
                    "This is not ideal for large files since a hash of each record is held in memory.");
    static final AllowableValue BLOOM_FILTER_VALUE = new AllowableValue("bloom-filter", "BloomFilter",
            "Space-efficient data structure ideal for large data sets uses probability to determine if a record was seen before. " +
                    "False positive matches are possible, but false negatives are not – in other words, a query returns either \"possibly in set\" or \"definitely not in set\". " +
                    "You should use this option if the FlowFile content is large and you can tolerate some duplication in the data.");


    // PROPERTIES

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for reading incoming data")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor INCLUDE_ZERO_RECORD_FLOWFILES = new PropertyDescriptor.Builder()
            .name("include-zero-record-flowfiles")
            .displayName("Include Zero Record FlowFiles")
            .description("When converting an incoming FlowFile, if the conversion results in no data, "
                    + "this property specifies whether or not a FlowFile will be sent to the corresponding relationship")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(true)
            .build();

    static final PropertyDescriptor CACHE_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("Cache The Entry Identifier")
            .description("When true this cause the processor to check for duplicates and cache the Entry Identifier. When false, "
                    + "the processor would only check for duplicates and not cache the Entry Identifier, requiring another "
                    + "processor to add identifiers to the distributed cache.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    static final PropertyDescriptor DISTRIBUTED_CACHE_SERVICE = new PropertyDescriptor.Builder()
            .name("Distributed Cache Service")
            .description("The Controller Service that is used to cache unique records, used to determine duplicates")
            .required(false)
            .identifiesControllerService(DistributedMapCacheClient.class)
            .build();

    static final PropertyDescriptor CACHE_ENTRY_IDENTIFIER = new PropertyDescriptor.Builder()
            .name("Cache Entry Identifier")
            .description(
                    "A FlowFile attribute, or the results of an Attribute Expression Language statement, which will be evaluated " +
                            "against a FlowFile in order to determine the cached filter type value used to identify duplicates.")
            .required(false)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(ResultType.STRING, true))
            .defaultValue("${hash.value}")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor AGE_OFF_DURATION = new PropertyDescriptor.Builder()
            .name("Age Off Duration")
            .description("Time interval to age off cached filters. When the cache expires, the entire filter and its values " +
                    "are destroyed. Leaving this value empty will cause the cached entries to never expire.")
            .required(false)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor RECORD_HASHING_ALGORITHM = new PropertyDescriptor.Builder()
            .name("Record Hashing Algorithm")
            .description("The algorithm used to hash the combined result of RecordPath values in the cache.")
            .allowableValues(
                    MD5_ALGORITHM_VALUE,
                    SHA1_ALGORITHM_VALUE,
                    SHA256_ALGORITHM_VALUE,
                    SHA512_ALGORITHM_VALUE
            )
            .defaultValue(SHA1_ALGORITHM_VALUE.getValue())
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .required(true)
            .build();

    static final PropertyDescriptor FILTER_TYPE = new PropertyDescriptor.Builder()
            .name("Filter Type")
            .description("The filter used to determine whether a record has been seen before based on the matching RecordPath criteria.")
            .allowableValues(
                    HASH_SET_VALUE,
                    BLOOM_FILTER_VALUE
            )
            .defaultValue(HASH_SET_VALUE.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor FILTER_CAPACITY_HINT = new PropertyDescriptor.Builder()
            .name("Filter Capacity Hint")
            .description("An estimation of the total number of unique records to be processed. " +
                    "The more accurate this number is leads to less resizing operations on a HashSet filter and fewer false negatives on a BloomFilter. " +
                    "Using the CountText processor block first and then using the ${text.line.count} attribute may be ideal for more dynamic estimates.")
            .defaultValue("25000")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor BLOOM_FILTER_FPP = new PropertyDescriptor.Builder()
            .name("BloomFilter Probability")
            .description("The desired false positive probability when using the BloomFilter filter type. " +
                    "Using a value of .05 for example, means we can be 95% certain that the element is in the filter, " +
                    "and there is a five-percent probability that the result is a false positive. " +
                    "When the filter returns false, we can be 100% certain that the element is not present.")
            .defaultValue("0.10")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NUMBER_VALIDATOR)
            .required(false)
            .build();


    // RELATIONSHIPS

    static final Relationship REL_DUPLICATE = new Relationship.Builder()
            .name("duplicate")
            .description("Records detected as duplicates in the FlowFile content will be routed to this relationship")
            .build();

    static final Relationship REL_NON_DUPLICATE = new Relationship.Builder()
            .name("non-duplicate")
            .description("If the record was not found in the cache, it will be routed to this relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If unable to communicate with the cache, the FlowFile will be penalized and routed to this relationship")
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    private final Serializer<String> keySerializer = new StringSerializer();
    private final Serializer<CacheValue> cacheValueSerializer = new CacheValueSerializer();
    private final Deserializer<CacheValue> cacheValueDeserializer = new CacheValueDeserializer();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(RECORD_READER);
        descriptors.add(RECORD_WRITER);
        descriptors.add(INCLUDE_ZERO_RECORD_FLOWFILES);
        descriptors.add(CACHE_IDENTIFIER);
        descriptors.add(CACHE_ENTRY_IDENTIFIER);
        descriptors.add(AGE_OFF_DURATION);
        descriptors.add(DISTRIBUTED_CACHE_SERVICE);
        descriptors.add(RECORD_HASHING_ALGORITHM);
        descriptors.add(FILTER_TYPE);
        descriptors.add(FILTER_CAPACITY_HINT);
        descriptors.add(BLOOM_FILTER_FPP);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_DUPLICATE);
        relationships.add(REL_NON_DUPLICATE);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Specifies a value to use from the record that match the RecordPath: '" +
                        propertyDescriptorName + "' which is used together with other specified " +
                        "record path values to determine the uniqueness of a record. " +
                        "Expression Language may reference variables 'field.name', 'field.type', and 'field.value' " +
                        "to access information about the field and the value of the field being evaluated.")
                .required(false)
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(new RecordPathPropertyNameValidator())
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        RecordPathValidator recordPathValidator = new RecordPathValidator();
        final List<ValidationResult> validationResults = validationContext.getProperties().keySet().stream()
                .filter(property -> property.isDynamic())
                .map(property -> recordPathValidator.validate(
                        "User-defined Properties",
                        property.getName(),
                        validationContext
                )).collect(Collectors.toList());

        if(validationContext.getProperty(BLOOM_FILTER_FPP).isSet()) {
            final int falsePositiveProbability = validationContext.getProperty(BLOOM_FILTER_FPP).asInteger();
            if (falsePositiveProbability < 0 || falsePositiveProbability > 1) {
                validationResults.add(
                        new ValidationResult.Builder()
                                .subject(BLOOM_FILTER_FPP.getName() + " out of range.")
                                .input(String.valueOf(falsePositiveProbability))
                                .explanation("Valid values are 0.0-1.0 inclusive")
                                .valid(false).build());
            }
        }

        if(validationContext.getProperty(CACHE_IDENTIFIER).asBoolean()) {
            if(!validationContext.getProperty(DISTRIBUTED_CACHE_SERVICE).isSet())
                validationResults.add(new ValidationResult.Builder()
                        .subject(DISTRIBUTED_CACHE_SERVICE.getName())
                        .explanation(DISTRIBUTED_CACHE_SERVICE.getName() + " is required when " + CACHE_IDENTIFIER.getName() + " is true.")
                        .valid(false).build());

            if(!validationContext.getProperty(CACHE_ENTRY_IDENTIFIER).isSet())
                validationResults.add(new ValidationResult.Builder()
                        .subject(CACHE_ENTRY_IDENTIFIER.getName())
                        .explanation(CACHE_ENTRY_IDENTIFIER.getName() + " is required when " + CACHE_IDENTIFIER.getName() + " is true.")
                        .valid(false).build());

            if(!validationContext.getProperty(AGE_OFF_DURATION).isSet())
                validationResults.add(new ValidationResult.Builder()
                        .subject(AGE_OFF_DURATION.getName())
                        .explanation(AGE_OFF_DURATION.getName() + " is required when " + CACHE_IDENTIFIER.getName() + " is true.")
                        .valid(false).build());
        }

        return validationResults;
    }

    @OnScheduled
    public void compileRecordPaths(final ProcessContext context) {
        final List<String> recordPaths = new ArrayList<>();

        recordPaths.addAll(context.getProperties().keySet().stream()
                .filter(property -> property.isDynamic())
                .map(PropertyDescriptor::getName)
                .collect(toList()));

        recordPathCache = new RecordPathCache(recordPaths.size());
        this.recordPaths = recordPaths;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final String cacheKey = context.getProperty(CACHE_ENTRY_IDENTIFIER).evaluateAttributeExpressions(flowFile).getValue();

        if (isBlank(cacheKey)) {
            logger.error("FlowFile {} has no attribute for given Cache Entry Identifier", new Object[]{flowFile});
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            return;
        }

        try {
            final long now = System.currentTimeMillis();
            final DistributedMapCacheClient cache = context.getProperty(DISTRIBUTED_CACHE_SERVICE).asControllerService(DistributedMapCacheClient.class);

            final boolean shouldCacheIdentifier = context.getProperty(CACHE_IDENTIFIER).asBoolean();
            final int filterCapacity = context.getProperty(FILTER_CAPACITY_HINT).asInteger();
            Serializable serializableFilter = context.getProperty(FILTER_TYPE).getValue()
                    .equals(context.getProperty(HASH_SET_VALUE.getValue()))
                    ? new HashSet<String>(filterCapacity)
                    : BloomFilter.create(
                    Funnels.stringFunnel(Charset.defaultCharset()),
                    filterCapacity,
                    context.getProperty(BLOOM_FILTER_FPP).asDouble());

            if(shouldCacheIdentifier && cache.containsKey(cacheKey, keySerializer)) {
                CacheValue cacheValue = cache.get(cacheKey, keySerializer, cacheValueDeserializer);
                Long durationMS = context.getProperty(AGE_OFF_DURATION).asTimePeriod(TimeUnit.MILLISECONDS);

                if(durationMS != null && (now >= cacheValue.getEntryTimeMS() + durationMS)) {
                    boolean status = cache.remove(cacheKey, keySerializer);
                    logger.debug("Removal of expired cached entry with key {} returned {}", new Object[]{cacheKey, status});
                } else {
                    serializableFilter = cacheValue.getFilter();
                }
            }

            final FilterWrapper filter = FilterWrapper.create(serializableFilter);

            final String recordHashingAlgorithm = context.getProperty(RECORD_HASHING_ALGORITHM).getValue();
            final MessageDigest messageDigest = DigestUtils.getDigest(recordHashingAlgorithm);
            final Boolean matchWholeRecord = context.getProperties().keySet().stream().noneMatch(p -> p.isDynamic());

            final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
            final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
            final RecordReader reader = readerFactory.createRecordReader(flowFile.getAttributes(), session.read(flowFile), logger);

            FlowFile nonDuplicatesFlowFile = session.create(flowFile);
            final FlowFile duplicatesFlowFile = session.create(flowFile);
            final RecordSchema writeSchema = writerFactory.getSchema(flowFile.getAttributes(), reader.getSchema());
            final RecordSetWriter nonDuplicatesWriter = writerFactory.createWriter(getLogger(), writeSchema, session.write(nonDuplicatesFlowFile));
            final RecordSetWriter duplicatesWriter = writerFactory.createWriter(getLogger(), writeSchema, session.write(duplicatesFlowFile));

            nonDuplicatesWriter.beginRecordSet();
            duplicatesWriter.beginRecordSet();
            Record record;

            while ((record = reader.nextRecord()) != null) {
                String recordValue;

                if (matchWholeRecord) {
                    recordValue = record.getSerializedForm().get().getSerialized().toString();
                } else {
                    final List<String> fieldValues = new ArrayList<>();
                    for (final String recordPathText : recordPaths) {
                        final PropertyValue recordPathPropertyValue = context.getProperty(recordPathText);
                        final RecordPath recordPath = recordPathCache.getCompiled(recordPathText);
                        final RecordPathResult result = recordPath.evaluate(record);
                        fieldValues.addAll(result.getSelectedFields()
                                        .map(f -> recordPathPropertyValue.evaluateAttributeExpressions(flowFile).getValue())
                                        .collect(toList())
                        );
                    }
                    recordValue = Joiner.on('~').join(fieldValues);
                }

                final String recordHash = Hex.encodeHexString(messageDigest.digest(getBytesUtf8(recordValue)));

                if(filter.contains(recordHash)) {
                    duplicatesWriter.write(record);
                } else {
                    nonDuplicatesWriter.write(record);
                }

                filter.put(recordHash);
            }

            final boolean includeZeroRecordFlowFiles = context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).isSet()
                    ? context.getProperty(INCLUDE_ZERO_RECORD_FLOWFILES).asBoolean()
                    : true;


            // Route Non-Duplicates FlowFile
            final WriteResult nonDuplicatesWriteResult = nonDuplicatesWriter.finishRecordSet();
            Map<String, String> attributes = new HashMap<>();
            attributes.putAll(nonDuplicatesWriteResult.getAttributes());
            attributes.put("record.count", String.valueOf(nonDuplicatesWriteResult.getRecordCount()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), nonDuplicatesWriter.getMimeType());
            nonDuplicatesFlowFile = session.putAllAttributes(nonDuplicatesFlowFile, attributes);
            logger.info("Successfully found {} unique records for {}", new Object[] {nonDuplicatesWriteResult.getRecordCount(), nonDuplicatesFlowFile});

            if(!includeZeroRecordFlowFiles && nonDuplicatesWriteResult.getRecordCount() == 0) {
                session.remove(nonDuplicatesFlowFile);
            } else {
                session.transfer(nonDuplicatesFlowFile, REL_NON_DUPLICATE);
            }

            // Route Duplicates FlowFile
            final WriteResult duplicatesWriteResult = duplicatesWriter.finishRecordSet();
            attributes.clear();
            attributes.putAll(duplicatesWriteResult.getAttributes());
            attributes.put("record.count", String.valueOf(duplicatesWriteResult.getRecordCount()));
            attributes.put(CoreAttributes.MIME_TYPE.key(), duplicatesWriter.getMimeType());
            nonDuplicatesFlowFile = session.putAllAttributes(nonDuplicatesFlowFile, attributes);
            logger.info("Successfully found {} duplicate records for {}", new Object[] {duplicatesWriteResult.getRecordCount(), nonDuplicatesFlowFile});

            if(!includeZeroRecordFlowFiles && duplicatesWriteResult.getRecordCount() == 0) {
                session.remove(duplicatesFlowFile);
            } else {
                session.transfer(duplicatesFlowFile, REL_DUPLICATE);
            }

            session.adjustCounter("Records Processed",
                    nonDuplicatesWriteResult.getRecordCount() + duplicatesWriteResult.getRecordCount(), false);

            if(shouldCacheIdentifier) {
                CacheValue cacheValue = new CacheValue(serializableFilter, now);
                cache.put(cacheKey, cacheValue, keySerializer, cacheValueSerializer);
            }

        } catch (final SchemaNotFoundException e) {
            throw new ProcessException(e.getLocalizedMessage(), e);
        } catch (final MalformedRecordException e) {
            throw new ProcessException("Could not parse incoming data", e);
        } catch (final IOException e) {
            session.transfer(session.penalize(flowFile), REL_FAILURE);
            logger.error("Unable to communicate with cache when processing {} due to {}", new Object[]{flowFile, e});
        } catch (final Exception e) {
            logger.error("Failed to process {}; will route to failure", new Object[]{flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
    }

    private abstract static class FilterWrapper {
        public static FilterWrapper create(Object filter) {
            if(filter instanceof HashSet) {
                return new HashSetFilterWrapper((HashSet<String>) filter);
            } else {
                return new BloomFilterWrapper((BloomFilter<String>) filter);
            }
        }
        public abstract boolean contains(String value);
        public abstract void put(String value);
    }

    private static class HashSetFilterWrapper extends FilterWrapper {

        private final HashSet<String> filter;

        public HashSetFilterWrapper(HashSet<String> filter) {
            this.filter = filter;
        }

        @Override
        public boolean contains(String value) {
            return filter.contains(value);
        }

        @Override
        public void put(String value) {
            filter.add(value);
        }
    }

    private static class BloomFilterWrapper extends FilterWrapper {

        private final BloomFilter<String> filter;

        public BloomFilterWrapper(BloomFilter<String> filter) {
            this.filter = filter;
        }

        @Override
        public boolean contains(String value) {
            return filter.mightContain(value);
        }

        @Override
        public void put(String value) {
            filter.put(value);
        }
    }

    private static class CacheValue implements Serializable {

        private final Serializable filter;
        private final long entryTimeMS;

        public CacheValue(Serializable filter, long entryTimeMS) {
            this.filter = filter;
            this.entryTimeMS = entryTimeMS;
        }

        public Serializable getFilter() {
            return filter;
        }

        public long getEntryTimeMS() {
            return entryTimeMS;
        }
    }

    private static class CacheValueSerializer implements Serializer<CacheValue> {
        @Override
        public void serialize(CacheValue cacheValue, OutputStream outputStream) throws SerializationException, IOException {
            new ObjectOutputStream(outputStream).writeObject(cacheValue);
        }
    }

    private static class CacheValueDeserializer implements Deserializer<CacheValue> {
        @Override
        public CacheValue deserialize(byte[] bytes) throws DeserializationException, IOException {
            try {
                return (CacheValue) new ObjectInputStream(new ByteArrayInputStream(bytes)).readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private static class StringSerializer implements Serializer<String> {

        @Override
        public void serialize(final String value, final OutputStream out) throws SerializationException, IOException {
            out.write(value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
