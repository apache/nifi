/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.kite;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetRecordException;
import org.kitesdk.data.SchemaNotFoundException;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.filesystem.JSONFileReader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

@Tags({"kite", "json", "avro"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts JSON files to Avro according to an Avro Schema")
public class ConvertJSONToAvro extends AbstractKiteConvertProcessor {

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Avro content that was converted successfully from JSON")
            .build();

    private static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("JSON content that could not be processed")
            .build();

    private static final Relationship INCOMPATIBLE = new Relationship.Builder()
            .name("incompatible")
            .description("JSON content that could not be converted")
            .build();

    @VisibleForTesting
    static final PropertyDescriptor SCHEMA
            = new PropertyDescriptor.Builder()
            .name("Record schema")
            .description("Outgoing Avro schema for each record created from a JSON object")
            .addValidator(SCHEMA_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES
            = ImmutableList.<PropertyDescriptor>builder()
            .addAll(AbstractKiteProcessor.getProperties())
            .add(SCHEMA)
            .add(COMPRESSION_TYPE)
            .build();

    private static final Set<Relationship> RELATIONSHIPS
            = ImmutableSet.<Relationship>builder()
            .add(SUCCESS)
            .add(FAILURE)
            .add(INCOMPATIBLE)
            .build();

    public ConvertJSONToAvro() {
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
    public void onTrigger(final ProcessContext context, final ProcessSession session)
            throws ProcessException {
        FlowFile incomingJSON = session.get();
        if (incomingJSON == null) {
            return;
        }

        String schemaProperty = context.getProperty(SCHEMA)
                .evaluateAttributeExpressions(incomingJSON)
                .getValue();
        final Schema schema;
        try {
            schema = getSchema(schemaProperty, DefaultConfiguration.get());
        } catch (SchemaNotFoundException e) {
            getLogger().error("Cannot find schema: " + schemaProperty);
            session.transfer(incomingJSON, FAILURE);
            return;
        }

        final DataFileWriter<Record> writer = new DataFileWriter<>(
                AvroUtil.newDatumWriter(schema, Record.class));
        writer.setCodec(getCodecFactory(context.getProperty(COMPRESSION_TYPE).getValue()));

        try {
            final AtomicLong written = new AtomicLong(0L);
            final FailureTracker failures = new FailureTracker();

            FlowFile badRecords = session.clone(incomingJSON);
            FlowFile outgoingAvro = session.write(incomingJSON, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    try (JSONFileReader<Record> reader = new JSONFileReader<>(
                            in, schema, Record.class)) {
                        reader.initialize();
                        try (DataFileWriter<Record> w = writer.create(schema, out)) {
                            while (reader.hasNext()) {
                                try {
                                    Record record = reader.next();
                                    w.append(record);
                                    written.incrementAndGet();

                                } catch (final DatasetRecordException e) {
                                    failures.add(e);
                                }
                            }
                        }
                    }
                }
            });

            long errors = failures.count();

            session.adjustCounter("Converted records", written.get(),
                    false /* update only if file transfer is successful */);
            session.adjustCounter("Conversion errors", errors,
                    false /* update only if file transfer is successful */);

            if (written.get() > 0L) {
                outgoingAvro = session.putAttribute(outgoingAvro, CoreAttributes.MIME_TYPE.key(), InferAvroSchema.AVRO_MIME_TYPE);
                session.transfer(outgoingAvro, SUCCESS);

                if (errors > 0L) {
                    getLogger().warn("Failed to convert {}/{} records from JSON to Avro",
                            new Object[] { errors, errors + written.get() });
                    badRecords = session.putAttribute(
                            badRecords, "errors", failures.summary());
                    session.transfer(badRecords, INCOMPATIBLE);
                } else {
                    session.remove(badRecords);
                }

            } else {
                session.remove(outgoingAvro);

                if (errors > 0L) {
                    getLogger().warn("Failed to convert {}/{} records from JSON to Avro",
                            new Object[] { errors, errors });
                    badRecords = session.putAttribute(
                            badRecords, "errors", failures.summary());
                } else {
                    badRecords = session.putAttribute(
                            badRecords, "errors", "No incoming records");
                }

                session.transfer(badRecords, FAILURE);
            }

        } catch (ProcessException | DatasetIOException e) {
            getLogger().error("Failed reading or writing", e);
            session.transfer(incomingJSON, FAILURE);
        } catch (DatasetException e) {
            getLogger().error("Failed to read FlowFile", e);
            session.transfer(incomingJSON, FAILURE);
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                getLogger().warn("Unable to close writer ressource", e);
            }
        }
    }

}
