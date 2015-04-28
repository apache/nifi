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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.io.StreamCallback;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetRecordException;
import org.kitesdk.data.SchemaNotFoundException;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.filesystem.JSONFileReader;

@Tags({"kite", "json", "avro"})
@CapabilityDescription(
        "Converts JSON files to Avro according to an Avro Schema")
public class ConvertJSONToAvro extends AbstractKiteProcessor {

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFile content has been successfully saved")
            .build();

    private static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFile content could not be processed")
            .build();

    @VisibleForTesting
    static final PropertyDescriptor SCHEMA
            = new PropertyDescriptor.Builder()
            .name("Record schema")
            .description("Outgoing Avro schema for each record created from a JSON object")
            .addValidator(SCHEMA_VALIDATOR)
            .expressionLanguageSupported(true)
            .required(true)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES
            = ImmutableList.<PropertyDescriptor>builder()
            .addAll(AbstractKiteProcessor.getProperties())
            .add(SCHEMA)
            .build();

    private static final Set<Relationship> RELATIONSHIPS
            = ImmutableSet.<Relationship>builder()
            .add(SUCCESS)
            .add(FAILURE)
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
        FlowFile successfulRecords = session.get();
        if (successfulRecords == null) {
            return;
        }

        String schemaProperty = context.getProperty(SCHEMA)
                .evaluateAttributeExpressions(successfulRecords)
                .getValue();
        final Schema schema;
        try {
            schema = getSchema(schemaProperty, DefaultConfiguration.get());
        } catch (SchemaNotFoundException e) {
            getLogger().error("Cannot find schema: " + schemaProperty);
            session.transfer(successfulRecords, FAILURE);
            return;
        }

        final DataFileWriter<Record> writer = new DataFileWriter<>(
                AvroUtil.newDatumWriter(schema, Record.class));
        writer.setCodec(CodecFactory.snappyCodec());

        try {
          successfulRecords = session.write(successfulRecords, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    FlowFile failedRecords = session.create();
                    long written = 0L;
                    long errors = 0L;
                    long total = 0L;
                    try (JSONFileReader<Record> reader = new JSONFileReader<>(
                            in, schema, Record.class)) {
                        reader.initialize();
                        try (DataFileWriter<Record> w = writer.create(schema, out)) {
                            while (reader.hasNext()) {
                                total += 1;
                                try {
                                    Record record = reader.next();
                                    w.append(record);
                                    written += 1;
                                } catch (final DatasetRecordException e) {
                                    failedRecords = session.append(failedRecords, new OutputStreamCallback() {
                                        @Override
                                        public void process(OutputStream out) throws IOException {
                                            out.write((e.getMessage() + " [" +
                                              e.getCause().getMessage() + "]\n").getBytes());
                                        }
                                    });
                                    errors += 1;
                                }
                            }
                        }
                        session.adjustCounter("Converted records", written,
                                false /* update only if file transfer is successful */);
                        session.adjustCounter("Conversion errors", errors,
                          false /* update only if file transfer is successful */);

                        if (errors > 0L) {
                            getLogger().warn("Failed to convert " + errors + '/' + total + " records from JSON to Avro");
                        }
                    }
                    session.transfer(failedRecords, FAILURE);
                }
            });

            session.transfer(successfulRecords, SUCCESS);

            //session.getProvenanceReporter().send(flowFile, target.getUri().toString());
        } catch (ProcessException | DatasetIOException e) {
            getLogger().error("Failed reading or writing", e);
            session.transfer(successfulRecords, FAILURE);

        } catch (DatasetException e) {
            getLogger().error("Failed to read FlowFile", e);
            session.transfer(successfulRecords, FAILURE);

        }
    }

}
