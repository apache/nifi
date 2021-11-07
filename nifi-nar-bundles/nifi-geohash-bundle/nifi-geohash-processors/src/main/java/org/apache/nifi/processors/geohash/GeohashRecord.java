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
package org.apache.nifi.processors.geohash;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
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
import org.apache.nifi.serialization.WriteResult;

import ch.hsr.geohash.GeoHash;
import ch.hsr.geohash.WGS84Point;

import java.io.InputStream;
import java.io.OutputStream;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"geo", "geohash", "record"})
@CapabilityDescription("A record-based processor that encodes and decodes Geohashes from and to latitude/longitude coordinates.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "The MIME type indicated by the record writer"),
        @WritesAttribute(attribute = "record.count", description = "The number of records in the resulting flow file")
})
public class GeohashRecord extends AbstractProcessor {

    public enum ProcessingMode {
        ENCODE, DECODE
    }

    public enum GeohashFormat {
        BASE32, BINARY, LONG
    }

    public enum RoutingStrategy {
        SKIP_UNENRICHED,
        SPLIT,
        REQUIRE_ALL_ENRICHED
    }

    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("mode")
            .displayName("Mode")
            .description("Specifies whether to encode latitude/longitude to geohash or decode geohash to latitude/longitude")
            .required(true)
            .allowableValues(ProcessingMode.values())
            .defaultValue(ProcessingMode.ENCODE.name())
            .build();

    public static final PropertyDescriptor ROUTING_STRATEGY = new PropertyDescriptor.Builder()
            .name("routing-strategy")
            .displayName("Routing Strategy")
            .description("Specifies how to route records after encoding or decoding has been performed. "
                    + "SKIP_UNENRICHED will route a flowfile to success if any of its records is enriched; otherwise, it will be sent to failure. "
                    + "SPLIT will separate the records that have been enriched from those that have not and send them to success, while unenriched records will be sent to failure; "
                    + "and the original flowfile will be sent to the original relationship. "
                    + "REQUIRE_ALL_ENRICHED will route a flowfile to success only if all of its records are enriched; otherwise, it will be sent to failure")
            .required(true)
            .allowableValues(RoutingStrategy.values())
            .defaultValue(RoutingStrategy.SKIP_UNENRICHED.name())
            .build();

    public static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the record reader service to use for reading incoming data")
            .required(true)
            .identifiesControllerService(RecordReaderFactory.class)
            .build();

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the record writer service to use for writing data")
            .required(true)
            .identifiesControllerService(RecordSetWriterFactory.class)
            .build();

    public static final PropertyDescriptor LATITUDE_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("latitude-record-path")
            .displayName("Latitude Record Path")
            .description("In the ENCODE mode, this property specifies the record path to retrieve the latitude value; "
                    + "in the DECODE mode, this property specifies the record path to put the latitude value")
            .required(true)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor LONGITUDE_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("longitude-record-path")
            .displayName("Longitude Record Path")
            .description("In the ENCODE mode, this property specifies the record path to retrieve the longitude value; "
                    + "in the DECODE mode, this property specifies the record path to put the longitude value")
            .required(true)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor GEOHASH_RECORD_PATH = new PropertyDescriptor.Builder()
            .name("geohash-record-path")
            .displayName("Geohash Record Path")
            .description("In the ENCODE mode, this property specifies the record path to put the geohash value; "
                    + "in the DECODE mode, this property specifies the record path to retrieve the geohash value")
            .required(true)
            .addValidator(new RecordPathValidator())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor GEOHASH_FORMAT = new PropertyDescriptor.Builder()
            .name("geohash-format")
            .displayName("Geohash Format")
            .description("In the ENCODE mode, this property specifies the desired format for encoding geohash; "
                    + "in the DECODE mode, this property specifies the format of geohash provided")
            .required(true)
            .allowableValues(GeohashFormat.values())
            .defaultValue(GeohashFormat.BASE32.name())
            .build();

    public static final PropertyDescriptor GEOHASH_LEVEL = new PropertyDescriptor.Builder()
            .name("geohash-level")
            .displayName("Geohash Level")
            .description("The integer precision level(1-12) desired for encoding geohash")
            .required(true)
            .addValidator(StandardValidators.createLongValidator(1, 12, true))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(MODE, ProcessingMode.ENCODE.name())
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Records that are successfully encoded or decoded will be routed to success")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Records that cannot be encoded or decoded will be routed to failure")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("With the SPLIT strategy, the original input flowfile will be sent to this relationship regardless of whether it was enriched or not.")
            .build();

    private static final List<PropertyDescriptor> RECORD_PATH_PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            LATITUDE_RECORD_PATH, LONGITUDE_RECORD_PATH, GEOHASH_RECORD_PATH
    ));

    private final RecordPathCache cache = new RecordPathCache(100);

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = new ArrayList<>();
        descriptors.add(MODE);
        descriptors.add(RECORD_READER);
        descriptors.add(RECORD_WRITER);
        descriptors.add(ROUTING_STRATEGY);
        descriptors.add(LATITUDE_RECORD_PATH);
        descriptors.add(LONGITUDE_RECORD_PATH);
        descriptors.add(GEOHASH_RECORD_PATH);
        descriptors.add(GEOHASH_FORMAT);
        descriptors.add(GEOHASH_LEVEL);
        descriptors = Collections.unmodifiableList(descriptors);

        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        relationships = Collections.unmodifiableSet(relationships);
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile input = session.get();
        if (input == null) {
            return;
        }

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final boolean encode = context.getProperty(MODE).getValue().equalsIgnoreCase(ProcessingMode.ENCODE.toString());
        final RoutingStrategy routingStrategy = RoutingStrategy.valueOf(context.getProperty(ROUTING_STRATEGY).getValue());
        final GeohashFormat format = GeohashFormat.valueOf(context.getProperty(GEOHASH_FORMAT).getValue());

        FlowFile output = session.create(input);
        FlowFile notFound = routingStrategy == RoutingStrategy.SPLIT ? session.create(input) : null;

        try (final InputStream is = session.read(input);
             final RecordReader reader = readerFactory.createRecordReader(input, is, getLogger());
             final OutputStream os = session.write(output);
             final OutputStream osNotFound = routingStrategy == RoutingStrategy.SPLIT ? session.write(notFound) : null) {

            final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writerFactory.getSchema(input.getAttributes(), reader.getSchema()), os, output);
            final RecordSetWriter notFoundWriter = routingStrategy == RoutingStrategy.SPLIT ? writerFactory.createWriter(getLogger(), reader.getSchema(), osNotFound, notFound) : null;

            Map<PropertyDescriptor, RecordPath> paths = new HashMap<>();
            for (PropertyDescriptor descriptor : RECORD_PATH_PROPERTIES) {
                String rawRecordPath = context.getProperty(descriptor).evaluateAttributeExpressions(input).getValue();
                RecordPath compiled = cache.getCompiled(rawRecordPath);
                paths.put(descriptor, compiled);
            }

            Record record;

            //The overall relationship used by the REQUIRE_ALL_ENRICHED and SKIP_UNENRICHED routing strategies to transfer Flowfiles.
            //For the SPLIT strategy, the transfer of Flowfiles does not rely on this overall relationship. Instead, each individual record will be sent to success or failure.
            Relationship targetRelationship = routingStrategy == RoutingStrategy.REQUIRE_ALL_ENRICHED ? REL_SUCCESS : REL_FAILURE;

            writer.beginRecordSet();

            if (notFoundWriter != null) {
                notFoundWriter.beginRecordSet();
            }

            int foundCount = 0;
            int notFoundCount = 0;

            int level = context.getProperty(GEOHASH_LEVEL).evaluateAttributeExpressions(input).asInteger();
            final String rawLatitudePath = context.getProperty(LATITUDE_RECORD_PATH).evaluateAttributeExpressions(input).getValue();
            RecordPath latitudePath = cache.getCompiled(rawLatitudePath);
            final String rawLongitudePath = context.getProperty(LONGITUDE_RECORD_PATH).evaluateAttributeExpressions(input).getValue();
            RecordPath longitudePath = cache.getCompiled(rawLongitudePath);
            final String rawgeohashPath = context.getProperty(GEOHASH_RECORD_PATH).evaluateAttributeExpressions(input).getValue();
            RecordPath geohashPath = cache.getCompiled(rawgeohashPath);

            while ((record = reader.nextRecord()) != null) {
                boolean updated = false;
                if (encode) {
                    Object encodedGeohash = getEncodedGeohash(latitudePath, longitudePath, record, format, level);
                    updated = updateRecord(GEOHASH_RECORD_PATH, encodedGeohash, record, paths);
                } else {
                    WGS84Point decodedPoint = getDecodedPointFromGeohash(geohashPath, record, format);
                    if (decodedPoint != null) {
                        updated = updateRecord(LATITUDE_RECORD_PATH, String.valueOf(decodedPoint.getLatitude()), record, paths)
                                && updateRecord(LONGITUDE_RECORD_PATH, String.valueOf(decodedPoint.getLongitude()), record, paths);
                    }
                }

                if (routingStrategy == RoutingStrategy.REQUIRE_ALL_ENRICHED) {
                    if (!updated) { //If the routing strategy is REQUIRE_ALL_ENRICHED and there exists a record that is not updated, the entire flowfile should be route to REL_FAILURE
                        targetRelationship = REL_FAILURE;
                    }
                } else {
                    if (updated) {
                        //If the routing strategy is SKIP_UNENRICHED and there exists a record that is updated, the entire flowfile should be route to REL_SUCCESS
                        //If the routing strategy is SPLIT and there exists a record that is updated, this record should be route to REL_SUCCESS
                        targetRelationship = REL_SUCCESS;
                    }
                }

                if (routingStrategy != RoutingStrategy.SPLIT || updated) {
                    writer.write(record);
                    foundCount++;
                } else { //if the routing strategy is SPLIT and the record is not updated
                    notFoundWriter.write(record);
                    notFoundCount++;
                }
            }

            final WriteResult writeResult = writer.finishRecordSet();
            writer.close();

            WriteResult notFoundWriterResult = null;

            if (notFoundWriter != null) {
                notFoundWriterResult = notFoundWriter.finishRecordSet();
                notFoundWriter.close();
            }

            output = session.putAllAttributes(output, buildAttributes(foundCount, writer.getMimeType(), writeResult));
            if (routingStrategy != RoutingStrategy.SPLIT) {
                session.transfer(output, targetRelationship);
                session.remove(input);
            } else {
                if (notFoundCount > 0) {
                    notFound = session.putAllAttributes(notFound, buildAttributes(notFoundCount, writer.getMimeType(), notFoundWriterResult));
                    session.transfer(notFound, REL_FAILURE);
                } else {
                    session.remove(notFound);
                }
                session.transfer(output, REL_SUCCESS);
                session.transfer(input, REL_ORIGINAL);
            }

        } catch (Exception ex) {
            getLogger().error("Failed to {} due to {}", encode ? "encode" : "decode", ex.getLocalizedMessage(), ex);
        }
    }


    private Object getEncodedGeohash(RecordPath latitudePath, RecordPath longitudePath, Record record, GeohashFormat format, int level) {
        RecordPathResult latitudeResult = latitudePath.evaluate(record);
        RecordPathResult longitudeResult = longitudePath.evaluate(record);
        Optional<FieldValue> latitudeField = latitudeResult.getSelectedFields().findFirst();
        Optional<FieldValue> longitudeField = longitudeResult.getSelectedFields().findFirst();

        if (!latitudeField.isPresent() || !longitudeField.isPresent()) {
            return null;
        }

        FieldValue latitudeValue = latitudeField.get();
        FieldValue longitudeValue = longitudeField.get();
        Object latitudeVal = latitudeValue.getValue();
        Object longitudeVal = longitudeValue.getValue();

        if (latitudeVal == null || longitudeVal == null) {
            return null;
        }

        double realLatValue = Double.parseDouble(latitudeVal.toString());
        double realLongValue = Double.parseDouble(longitudeVal.toString());
        GeoHash gh = GeoHash.withCharacterPrecision(realLatValue, realLongValue, level);

        switch (format) {
            case BINARY:
                return gh.toBinaryString();
            case LONG:
                return gh.longValue();
            default:
                return gh.toBase32();
        }
    }

    private WGS84Point getDecodedPointFromGeohash(RecordPath geohashPath, Record record, GeohashFormat format) {
        RecordPathResult geohashResult = geohashPath.evaluate(record);
        Optional<FieldValue> geohashField = geohashResult.getSelectedFields().findFirst();

        if (!geohashField.isPresent()) {
            return null;
        }

        FieldValue geohashFieldValue = geohashField.get();
        Object geohashVal = geohashFieldValue.getValue();
        if (geohashVal == null) {
            return null;
        }

        String geohashString = geohashVal.toString();
        GeoHash decodedHash;

        switch (format) {
            case BINARY:
                decodedHash = GeoHash.fromBinaryString(geohashString);
                break;
            case LONG:
                String binaryString = Long.toBinaryString(Long.parseLong(geohashString));
                decodedHash = GeoHash.fromBinaryString(binaryString);
                break;
            default:
                decodedHash = GeoHash.fromGeohashString(geohashString);
        }

        return decodedHash.getBoundingBoxCenter();
    }

    private boolean updateRecord(PropertyDescriptor descriptor, Object newValue, Record record, Map<PropertyDescriptor, RecordPath> cached) {
        if (!cached.containsKey(descriptor) || newValue == null) {
            return false;
        }
        RecordPath path = cached.get(descriptor);
        RecordPathResult result = path.evaluate(record);

        final Optional<FieldValue> fieldValueOption = result.getSelectedFields().findFirst();
        if (!fieldValueOption.isPresent()) {
            return false;
        }

        final FieldValue fieldValue = fieldValueOption.get();

        if (!fieldValue.getParent().isPresent() || fieldValue.getParent().get().getValue() == null) {
            return false;
        }

        fieldValue.updateValue(newValue);
        return true;
    }

    private Map<String, String> buildAttributes(int recordCount, String mimeType, WriteResult writeResult) {
        Map<String, String> retVal = new HashMap<>();
        retVal.put(CoreAttributes.MIME_TYPE.key(), mimeType);
        retVal.put("record.count", String.valueOf(recordCount));
        retVal.putAll(writeResult.getAttributes());
        return retVal;
    }
}