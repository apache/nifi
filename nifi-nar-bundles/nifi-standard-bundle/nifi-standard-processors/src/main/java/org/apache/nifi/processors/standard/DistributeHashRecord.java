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

import com.sangupta.murmur.Murmur2;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.ProcessSession;

import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Comparator;


@SideEffectFree
@Tags({"route", "distribute", "weighted", "record"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Processor that distribute data over user specified relationships by distribution key/keys." +
        " Data is distributed across relationships in the amount proportional to the relationship weight. For example, if there are " +
        "two relationships and the first has a weight of 9 while the second has a weight of 10, the first will be sent 9 / 19 parts" +
        " of the rows, and the second will be sent 10 / 19. " +
        "To select the relationship that a row of data is sent to, specified keys extracted from record as string," +
        " join with `-` delimiter and hash evaluate from this string, its remainder is taken from dividing it " +
        "by the total weight of the relationships. If there is specified single integer key, hash value will not be calculated " +
        "and processor just take remainder of division by the sum of the relationship weights from this value." +
        " The row is sent to the relationship" +
        " that corresponds to the half-interval of the remainders from 'prev_weight' to 'prev_weights + weight', where" +
        " 'prev_weights' is the total weight of the relationships with the smallest number, and 'weight' is the weight of this relationship." +
        " For example, if there are two relationships, and the first has a weight of 9 while the second has a weight of 10," +
        " the row will be sent to the first relationship for the remainders from the range [0, 9), and to the second for the remainders from the range [9, 19).")
@DynamicProperty(name = "The name of the relationship to route data to",
        value = "Weight for this relationship.",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Each user-defined property specifies a relationship and weight for this.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer"),
        @WritesAttribute(attribute = "record.count", description = "The number of records selected by the query")
})
public class DistributeHashRecord extends AbstractProcessor {

    public static final String MURMURHASH_32 = "murmurhash_32";
    public static final String MURMURHASH_64 = "murmurhash_64";
    public static final String MD5 = "MD5";
    public static final String SHA1 = "SHA1";

    public static PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("reader")
            .displayName("Record Reader")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .build();

    public static PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("writer")
            .displayName("Record Writer")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .description("Specifies the Controller Service to use for writing out the records")
            .build();

    private static final String KEY_DELIMITER = ",";

    public static PropertyDescriptor KEYS = new PropertyDescriptor.Builder()
            .name("keys")
            .displayName("Keys")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .description("Field names in a record separated by commas." +
                    " If record has one key and this key is integer or long then hash function " +
                    "will not be evaluated and processor will distribute record by this numerical value. " +
                    "If this record has several keys for distribution or one key with not 'int' or 'long' type then processor will " +
                    "obtain keys from record, trim them and join with `-` delimiter like <firstKey>-<secondKey>-<...> " +
                    "then evaluate hash function which return numerical value for distribution")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static PropertyDescriptor HASH_FUNCTION = new PropertyDescriptor.Builder()
            .name("hash function")
            .displayName("Hash Function")
            .required(true)
            .description("Hash algorithm for keys hashing")
            .allowableValues(MURMURHASH_32, MURMURHASH_64, MD5, SHA1)
            .defaultValue(MURMURHASH_32)
            .build();

    public static PropertyDescriptor NULL_REPLACEMENT_VALUE = new PropertyDescriptor.Builder()
            .name("null replacement value")
            .displayName("Null replacement value")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .description("Replacement value for null fields obtained from record by keys.\n" +
                    "If value doesn't set then processor will throw DistributionException if it encounter null for key fields.")
            .addValidator(Validator.VALID)
            .build();


    public static Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The original FlowFile is routed to this relationship in case of success")
            .build();

    public static Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("If a Processor fails processing for any reason, the original FlowFile will "
                    + "be routed to this relationship")
            .build();

    private List<PropertyDescriptor> propertyDescriptors;
    private Set<Relationship> relationships;
    private List<Relationship> weightedRelationships;

    protected void init(final ProcessorInitializationContext context) {
        relationships = new HashSet<>();
        relationships.add(FAILURE);
        relationships.add(ORIGINAL);
        propertyDescriptors = Arrays.asList(
                RECORD_READER,
                RECORD_WRITER,
                KEYS,
                NULL_REPLACEMENT_VALUE,
                HASH_FUNCTION);
        weightedRelationships = new LinkedList<>();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {
        if (!descriptor.isDynamic()) {
            return;
        }
        Relationship relationship = new Relationship.Builder()
                .name(descriptor.getName())
                .description("User-defined relationship that specifies where data should be routed")
                .build();

        if (newValue == null) {
            relationships.remove(relationship);
        } else {
            relationships.add(relationship);
        }
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .dynamic(true)
                .addValidator(StandardValidators.INTEGER_VALIDATOR)
                .description("Specifies the weight and relationship for FlowFiles")
                .build();
    }

    @OnScheduled
    public void onScheduled(ProcessContext context) {
        weightedRelationships = weightRels(context.getProperties());
    }

    protected List<Relationship> weightRels(Map<PropertyDescriptor, String> properties){
        return properties.entrySet().stream()
                .filter(entry -> entry.getKey().isDynamic())
                .sorted(Comparator.comparing(e -> e.getKey().getName()))
                .flatMap(entry -> {
                    PropertyDescriptor descriptor = entry.getKey();
                    Relationship relationship = new Relationship.Builder()
                            .name(descriptor.getName())
                            .description("User-defined relationship that specifies where data should be routed")
                            .build();
                    return Stream.generate(() -> relationship).limit(Integer.parseInt(entry.getValue()));
                }).collect(Collectors.toList());
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException{
        FlowFile flowFile = session.get();
        if (flowFile == null)
            return;
        List<String> keys = extractKeys(context.getProperty(KEYS).evaluateAttributeExpressions(flowFile).getValue(), KEY_DELIMITER);
        String replacementValue = context.getProperty(NULL_REPLACEMENT_VALUE).evaluateAttributeExpressions(flowFile).getValue();
        RecordReaderFactory readerFactory = context.getProperty(RECORD_READER)
                .asControllerService(RecordReaderFactory.class);
        RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER)
                .asControllerService(RecordSetWriterFactory.class);

        Map<Relationship, OpenedFile> routing = new HashMap<>();
        try (InputStream in = session.read(flowFile)) {
            RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger());
            RecordSchema readerSchema = reader.getSchema();
            validateKeys(keys, readerSchema);
            Record record;
            while ((record = reader.nextRecord()) != null) {
                Relationship rel = distribute(keys, replacementValue, weightedRelationships, record, context.getProperty(HASH_FUNCTION).getValue());
                if (routing.get(rel) == null) {
                    OpenedFile openedFile = openFile(session, flowFile, readerSchema, writerFactory);
                    routing.put(rel, openedFile);
                }
                routing.get(rel).write(record);
            }
            for (Map.Entry<Relationship, OpenedFile> entry : routing.entrySet()) {
                OpenedFile file = entry.getValue();
                file.close();
                session.putAttribute(file.flowFile, "record.count", String.valueOf(file.recordCount()));
                file.mimeType().map(mimeType -> session.putAttribute(file.flowFile, "mime.type", mimeType));
                session.transfer(entry.getValue().flowFile, entry.getKey());
            }
            routing.clear();
            in.close();
            session.transfer(flowFile, ORIGINAL);
        } catch (Exception e) {
            getLogger().error(e.getMessage());
            session.transfer(flowFile, FAILURE);
        } finally {
            //only in case failure
            for (Map.Entry<Relationship, OpenedFile> entry : routing.entrySet()) {
                try {
                    entry.getValue().close();
                    session.remove(entry.getValue().flowFile);
                } catch (IOException e) {
                    getLogger().error(e.getMessage());
                }
            }
        }
    }

    protected List<String> extractKeys(String rawKeys, String delimiter){
        if (rawKeys==null || rawKeys.isEmpty())
            return Collections.emptyList();
        return Arrays.stream(rawKeys.split(delimiter))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    private BigInteger hash(String value, String hashFunctionName) {
        if (value == null) {
            getLogger().error("Error occurs during evaluate hash value: Key value is null ");
            throw new NullPointerException();
        }
        switch (hashFunctionName) {
            case MURMURHASH_32:
                return BigInteger.valueOf(Murmur2.hash(value.getBytes(), value.length(), 0));
            case MURMURHASH_64:
                return BigInteger.valueOf(Murmur2.hash64(value.getBytes(), value.length(), 0));
            case MD5:
                return new BigInteger(DigestUtils.md2Hex(value), 16);
            case SHA1:
                return new BigInteger(DigestUtils.sha1Hex(value), 16);
        }
        return BigInteger.valueOf(0);
    }

    private OpenedFile openFile(ProcessSession session,
                               FlowFile srcFlowFile,
                               RecordSchema schema,
                               RecordSetWriterFactory writerFactory) throws IOException, SchemaNotFoundException {
        FlowFile outFlowFile = session.create(srcFlowFile);
        OutputStream outputStream = session.write(outFlowFile);
        RecordSetWriter writer = writerFactory.createWriter(getLogger(), schema, outputStream, outFlowFile);
        return new OpenedFile(outFlowFile, writer);
    }

    protected Relationship distribute(List<String> keys,
                                   String replacementValue,
                                   List<Relationship> weightedRelationships,
                                   Record record,
                                   String hashFunctionName) {
        Optional<BigInteger> optionalHash = Optional.empty();
        if (keys.size() == 1 && record.getValue(keys.get(0)) instanceof Number) {
            optionalHash = Optional.of(BigInteger.valueOf(record.getAsLong(keys.get(0))));
        }
        if (!optionalHash.isPresent()) {
            List<String> values = new ArrayList<>(record.getSchema().getFieldCount());
            for (String key : keys){
                Object rawValue = record.getValue(key);
                String value = rawValue == null ? replacementValue : rawValue.toString();
                if (value==null){
                    throw new DistributeException(String.format("Key '%s' is null and replacement value not specified", key));
                }
                values.add(value);
            }
            //delimiter is a good candidate for parametrize
            String finalKey = String.join("-", values);
            optionalHash = Optional.of(hash(finalKey, hashFunctionName));
        }
        BigInteger relsCount = BigInteger.valueOf(weightedRelationships.size());
        return weightedRelationships.get(optionalHash.get().remainder(relsCount).intValue());
    }

    protected void validateKeys(List<String> keys, RecordSchema schema){
        if (keys.size()==0)
            throw new DistributeException("`keys` value is empty", new NullPointerException());
        List<String> nonexistentKeys = nonexistentKeys(keys, schema);
        if (nonexistentKeys.size()>0)
            throw new DistributeException(String.format("Schema doesn't have next keys: [%s]",
                    String.join(", ", nonexistentKeys)));
    }

    protected List<String> nonexistentKeys(List<String> keys, RecordSchema schema){
        List<String> schemaFields = schema.getFieldNames();
        return keys.stream()
                .filter(key -> !schemaFields.contains(key))
                .collect(Collectors.toList());
    }

    private static class OpenedFile implements Closeable {
        final FlowFile flowFile;
        final RecordSetWriter writer;
        private int recordCount = 0;
        private final Optional<String> mimeType;

        public OpenedFile(FlowFile flowFile, RecordSetWriter writer) {
            this.flowFile = flowFile;
            this.writer = writer;
            this.mimeType = Optional.ofNullable(writer.getMimeType());
        }

        public void write(Record record) throws IOException {
            writer.write(record);
            recordCount++;
        }

        public Optional<String> mimeType() {
            return this.mimeType;
        }

        public int recordCount() {
            return recordCount;
        }

        @Override
        public void close() throws IOException {
            this.writer.close();
        }
    }

    protected class DistributeException extends ProcessException{

        public DistributeException(String message) {
            super(message);
        }

        public DistributeException(String message, Throwable cause){
            super(message, cause);
        }
    }
}
