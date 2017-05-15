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
package org.apache.nifi.processors.avro;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@SideEffectFree
@SupportsBatching
@Tags({"avro", "convert", "json"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts a Binary Avro record into a JSON object. This processor provides a direct mapping of an Avro field to a JSON field, such "
    + "that the resulting JSON will have the same hierarchical structure as the Avro document. Note that the Avro schema information will be lost, as this "
    + "is not a translation from binary Avro to JSON formatted Avro. The output JSON is encoded the UTF-8 encoding. If an incoming FlowFile contains a stream of "
    + "multiple Avro records, the resultant FlowFile will contain a JSON Array containing all of the Avro records or a sequence of JSON Objects.  If an incoming FlowFile does "
    + "not contain any records, an empty JSON object is the output. Empty/Single Avro record FlowFile inputs are optionally wrapped in a container as dictated by 'Wrap Single Record'")
@WritesAttribute(attribute = "mime.type", description = "Sets the mime type to application/json")
public class ConvertAvroToJSON extends AbstractProcessor {
    protected static final String CONTAINER_ARRAY = "array";
    protected static final String CONTAINER_NONE = "none";

    private static final byte[] EMPTY_JSON_OBJECT = "{}".getBytes(StandardCharsets.UTF_8);

    static final PropertyDescriptor CONTAINER_OPTIONS = new PropertyDescriptor.Builder()
        .name("JSON container options")
        .description("Determines how stream of records is exposed: either as a sequence of single Objects (" + CONTAINER_NONE
            + ") (i.e. writing every Object to a new line), or as an array of Objects (" + CONTAINER_ARRAY + ").")
        .allowableValues(CONTAINER_NONE, CONTAINER_ARRAY)
        .required(true)
        .defaultValue(CONTAINER_ARRAY)
        .build();
    static final PropertyDescriptor WRAP_SINGLE_RECORD = new PropertyDescriptor.Builder()
        .name("Wrap Single Record")
        .description("Determines if the resulting output for empty records or a single record should be wrapped in a container array as specified by '" + CONTAINER_OPTIONS.getName() + "'")
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();
    static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
        .name("Avro schema")
        .description("If the Avro records do not contain the schema (datum only), it must be specified here.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(false)
        .build();
    static final PropertyDescriptor USE_AVRO_JSON = new PropertyDescriptor.Builder().name("Use AVRO JSON")
        .description("Determines if the resulting JSON output is in AVRO-JSON format or standard JSON format. Default 'false'")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .required(true)
        .defaultValue("false")
        .allowableValues("false", "true")
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("A FlowFile is routed to this relationship after it has been converted to JSON")
        .build();
    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("A FlowFile is routed to this relationship if it cannot be parsed as Avro or cannot be converted to JSON for any reason")
        .build();

    private final static List<PropertyDescriptor> PROPERTIES;

    private final static Set<Relationship> RELATIONSHIPS;

    static {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONTAINER_OPTIONS);
        properties.add(WRAP_SINGLE_RECORD);
        properties.add(SCHEMA);
        properties.add(USE_AVRO_JSON);
        PROPERTIES = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationships);
    }

    private volatile Schema schema;

    private volatile boolean useContainer;

    private volatile boolean wrapSingleRecord;

    private volatile boolean useAvroJson;

    @OnScheduled
    public void schedule(ProcessContext context) {
        String containerOption = context.getProperty(CONTAINER_OPTIONS).getValue();
        this.useContainer = containerOption.equals(CONTAINER_ARRAY);
        this.wrapSingleRecord = context.getProperty(WRAP_SINGLE_RECORD).asBoolean() && useContainer;
        String stringSchema = context.getProperty(SCHEMA).getValue();
        this.schema = stringSchema == null ? null : new Schema.Parser().parse(stringSchema);
        this.useAvroJson = context.getProperty(USE_AVRO_JSON).asBoolean();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(final InputStream rawIn, final OutputStream rawOut) throws IOException {
                    final GenericData genericData = GenericData.get();

                    try (OutputStream out = new BufferedOutputStream(rawOut); InputStream in = new BufferedInputStream(rawIn)) {
                        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
                        if (schema != null) {
                            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
                            GenericRecord currRecord = reader.read(null, decoder);
                            if (useContainer && wrapSingleRecord) {
                                out.write('[');
                            }
                            byte[] outputBytes = (currRecord == null) ? EMPTY_JSON_OBJECT
                                    : (useAvroJson ? toAvroJSON(schema, currRecord) : genericData.toString(currRecord).getBytes(StandardCharsets.UTF_8));
                            out.write(outputBytes);
                            if (useContainer && wrapSingleRecord) {
                                out.write(']');
                            }
                        } else {
                            try (DataFileStream<GenericRecord> stream = new DataFileStream<>(in, reader)) {
                                int recordCount = 0;
                                GenericRecord currRecord = null;
                                if (stream.hasNext()) {
                                    currRecord = stream.next();
                                    recordCount++;
                                }
                                if (stream.hasNext() && useContainer || wrapSingleRecord) {
                                    out.write('[');
                                }
                                byte[] outputBytes = (currRecord == null) ? EMPTY_JSON_OBJECT
                                        : (useAvroJson ? toAvroJSON(stream.getSchema(), currRecord) : genericData.toString(currRecord).getBytes(StandardCharsets.UTF_8));
                                out.write(outputBytes);
                                while (stream.hasNext()) {
                                    if (useContainer) {
                                        out.write(',');
                                    } else {
                                        out.write('\n');
                                    }

                                    currRecord = stream.next(currRecord);
                                    out.write(genericData.toString(currRecord).getBytes(StandardCharsets.UTF_8));
                                    recordCount++;
                                }
                                if (recordCount > 1 && useContainer || wrapSingleRecord) {
                                    out.write(']');
                                }
                            }
                        }
                    }
                }
            });
        } catch (final ProcessException pe) {
            getLogger().error("Failed to convert {} from Avro to JSON due to {}; transferring to failure", new Object[]{flowFile, pe});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/json");
        session.transfer(flowFile, REL_SUCCESS);
    }

    private byte[] toAvroJSON(Schema schemaToUse, GenericRecord datum) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schemaToUse);
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schemaToUse, bos);
        writer.write(datum, encoder);
        encoder.flush();
        bos.flush();
        return bos.toByteArray();
    }
}
