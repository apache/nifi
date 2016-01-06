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
import java.io.BufferedReader;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.CSVUtil;
import org.kitesdk.shaded.com.google.common.collect.ImmutableSet;


@Tags({"kite", "csv", "avro", "infer", "schema"})
@SeeAlso({InferAvroSchemaFromCSV.class})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Creates an Avro schema from a CSV file header. The header line definition can either be provided" +
        "as a property to the processor OR present in the first line of CSV in the incoming FlowFile content. If a header" +
        " property is specified for this processor no attempt will be made to use the header line that may be present" +
        " in the incoming CSV FlowFile content.")
public class InferAvroSchemaFromCSV
        extends AbstractKiteProcessor {

    public static final String CSV_DELIMITER = ",";

    public static final PropertyDescriptor HEADER_LINE = new PropertyDescriptor.Builder()
            .name("CSV Header Line")
            .description("Comma separated string defining the column names expected in the CSV data. " +
                    "EX: \"fname,lname,zip,address\"")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor HEADER_LINE_SKIP_COUNT = new PropertyDescriptor.Builder()
            .name("CSV Header Line Skip Count")
            .description("Specifies the number of header lines that should be skipped when reading the CSV data. If the " +
                    " first line of the CSV data is a header line and you specify the \"CSV Header Line\" property " +
                    "you need to set this vlaue to 1 otherwise the header line will be treated as actual data.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ESCAPE_STRING = new PropertyDescriptor.Builder()
            .name("CSV escape string")
            .description("String that represents an escape sequence in the CSV FlowFile content data.")
            .required(true)
            .defaultValue("\\")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUOTE_STRING = new PropertyDescriptor.Builder()
            .name("CSV quote string")
            .description("String that represents a literal quote character in the CSV FlowFile content data.")
            .required(true)
            .defaultValue("'")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_NAME = new PropertyDescriptor.Builder()
            .name("Avro Record Name")
            .description("Value to be placed in the Avro record schema \"name\" field.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Charset")
            .description("Character encoding of CSV data.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRETTY_AVRO_OUTPUT = new PropertyDescriptor.Builder()
            .name("Pretty Avro Output")
            .description("If true the Avro output will be formatted.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully created Avro schema for CSV data.").build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
            .description("Original incoming FlowFile CSV data").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to create Avro schema for CSV data.").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HEADER_LINE);
        properties.add(HEADER_LINE_SKIP_COUNT);
        properties.add(ESCAPE_STRING);
        properties.add(QUOTE_STRING);
        properties.add(PRETTY_AVRO_OUTPUT);
        properties.add(RECORD_NAME);
        properties.add(CHARSET);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }


    @Override
    public void onTrigger(final ProcessContext context, ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        try {

            //Determines the header line either from the property input or the first line of the delimited file.
            final AtomicReference<String> header = new AtomicReference<>();
            final AtomicReference<Boolean> hasHeader = new AtomicReference<>();

            if (context.getProperty(HEADER_LINE).isSet()) {
                header.set(context.getProperty(HEADER_LINE).getValue());
                hasHeader.set(Boolean.FALSE);
            } else {
                //Read the first line of the file to get the header value.
                session.read(original, new InputStreamCallback() {
                    @Override
                    public void process(InputStream in) throws IOException {
                        BufferedReader br = new BufferedReader(new InputStreamReader(in));
                        header.set(br.readLine());
                        hasHeader.set(Boolean.TRUE);
                        br.close();
                    }
                });
            }

            //Prepares the CSVProperties for kite
            final CSVProperties props = new CSVProperties.Builder()
                    .delimiter(CSV_DELIMITER)
                    .escape(context.getProperty(ESCAPE_STRING).getValue())
                    .quote(context.getProperty(QUOTE_STRING).getValue())
                    .header(header.get())
                    .hasHeader(hasHeader.get())
                    .linesToSkip(context.getProperty(HEADER_LINE_SKIP_COUNT).asInteger())
                    .charset(context.getProperty(CHARSET).getValue())
                    .build();

            final Set<String> required = ImmutableSet.of();
            final AtomicReference<String> avroSchema = new AtomicReference<>();

            session.read(original, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    avroSchema.set(CSVUtil
                            .inferNullableSchema(
                                    context.getProperty(RECORD_NAME).getValue(), in, props, required)
                            .toString(context.getProperty(PRETTY_AVRO_OUTPUT).asBoolean()));
                }
            });

            FlowFile avroSchemaFF = session.write(session.create(), new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(avroSchema.get().getBytes());
                }
            });

            //Transfer the sessions.
            session.transfer(original, REL_ORIGINAL);
            session.transfer(avroSchemaFF, REL_SUCCESS);

        } catch (Exception ex) {
            getLogger().error(ex.getMessage());
            session.transfer(original, REL_FAILURE);
        }
    }
}
