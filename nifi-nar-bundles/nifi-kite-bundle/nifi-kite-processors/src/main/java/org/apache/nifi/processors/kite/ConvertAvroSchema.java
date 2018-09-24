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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.commons.lang.LocaleUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.kite.AvroRecordConverter.AvroConversionException;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.SchemaNotFoundException;
import org.kitesdk.data.spi.DefaultConfiguration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

@Tags({ "avro", "convert", "kite" })
@CapabilityDescription("Convert records from one Avro schema to another, including support for flattening and simple type conversions")
@InputRequirement(Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "Field name from input schema",
value = "Field name for output schema",
description = "Explicit mappings from input schema to output schema, which supports renaming fields and stepping into nested records on the input schema using notation like parent.id")
public class ConvertAvroSchema extends AbstractKiteConvertProcessor {

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Avro content that converted successfully").build();

    private static final Relationship FAILURE = new Relationship.Builder()
            .name("failure").description("Avro content that failed to convert")
            .build();

    /**
     * Makes sure the output schema is a valid output schema and that all its
     * fields can be mapped either automatically or are explicitly mapped.
     */
    protected static final Validator MAPPED_SCHEMA_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String uri,
                ValidationContext context) {
            Configuration conf = getConfiguration(context.getProperty(
                    CONF_XML_FILES).getValue());
            String inputUri = context.getProperty(INPUT_SCHEMA).getValue();
            String error = null;

            final boolean elPresent = context
                    .isExpressionLanguageSupported(subject)
                    && context.isExpressionLanguagePresent(uri);
            if (!elPresent) {
                try {
                    Schema outputSchema = getSchema(uri, conf);
                    Schema inputSchema = getSchema(inputUri, conf);
                    // Get the explicitly mapped fields. This is identical to
                    // logic in onTrigger, but ValidationContext and
                    // ProcessContext share no ancestor, so we cannot generalize
                    // the code.
                    Map<String, String> fieldMapping = new HashMap<>();
                    for (final Map.Entry<PropertyDescriptor, String> entry : context
                            .getProperties().entrySet()) {
                        if (entry.getKey().isDynamic()) {
                            fieldMapping.put(entry.getKey().getName(),
                                    entry.getValue());
                        }
                    }
                    AvroRecordConverter converter = new AvroRecordConverter(
                            inputSchema, outputSchema, fieldMapping);
                    Collection<String> unmappedFields = converter
                            .getUnmappedFields();
                    if (unmappedFields.size() > 0) {
                        error = "The following fields are unmapped: "
                                + unmappedFields;
                    }

                } catch (SchemaNotFoundException e) {
                    error = e.getMessage();
                }
            }
            return new ValidationResult.Builder().subject(subject).input(uri)
                    .explanation(error).valid(error == null).build();
        }
    };

    public static final String DEFAULT_LOCALE_VALUE = "default";
    public static final Validator LOCALE_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            String reason = null;
            if (!value.equals(DEFAULT_LOCALE_VALUE)) {
                try {
                    final Locale locale = LocaleUtils.toLocale(value);
                    if (locale == null) {
                        reason = "null locale returned";
                    } else if (!LocaleUtils.isAvailableLocale(locale)) {
                        reason = "locale not available";
                    }
                } catch (final IllegalArgumentException e) {
                    reason = "invalid format for locale";
                }
            }
            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };

    @VisibleForTesting
    static final PropertyDescriptor INPUT_SCHEMA = new PropertyDescriptor.Builder()
            .name("Input Schema")
            .description("Avro Schema of Input Flowfiles.  This can be a URI (dataset, view, or resource) or literal JSON schema.")
            .addValidator(SCHEMA_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    @VisibleForTesting
    static final PropertyDescriptor OUTPUT_SCHEMA = new PropertyDescriptor.Builder()
            .name("Output Schema")
            .description("Avro Schema of Output Flowfiles.  This can be a URI (dataset, view, or resource) or literal JSON schema.")
            .addValidator(MAPPED_SCHEMA_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true).build();

    @VisibleForTesting
    static final PropertyDescriptor LOCALE = new PropertyDescriptor.Builder()
            .name("Locale")
            .description("Locale to use for scanning data (see https://docs.oracle.com/javase/7/docs/api/java/util/Locale.html)" +
                    "or \" " + DEFAULT_LOCALE_VALUE + "\" for JVM default")
            .addValidator(LOCALE_VALIDATOR)
            .defaultValue(DEFAULT_LOCALE_VALUE).build();

    private static final List<PropertyDescriptor> PROPERTIES = ImmutableList
            .<PropertyDescriptor> builder()
            .add(INPUT_SCHEMA)
            .add(OUTPUT_SCHEMA)
            .add(LOCALE)
            .add(COMPRESSION_TYPE)
            .build();

    private static final Set<Relationship> RELATIONSHIPS = ImmutableSet
            .<Relationship> builder().add(SUCCESS).add(FAILURE).build();

    private static final Pattern AVRO_FIELDNAME_PATTERN = Pattern
            .compile("[A-Za-z_][A-Za-z0-9_\\.]*");

    /**
     * Validates that the input and output fields (from dynamic properties) are
     * all valid avro field names including "." to step into records.
     */
    protected static final Validator AVRO_FIELDNAME_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject,
                final String value, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject)
                    && context.isExpressionLanguagePresent(value)) {
                return new ValidationResult.Builder().subject(subject)
                        .input(value)
                        .explanation("Expression Language Present").valid(true)
                        .build();
            }

            String reason = "";
            if (!AVRO_FIELDNAME_PATTERN.matcher(subject).matches()) {
                reason = subject + " is not a valid Avro fieldname";
            }
            if (!AVRO_FIELDNAME_PATTERN.matcher(value).matches()) {
                reason = reason + value + " is not a valid Avro fieldname";
            }

            return new ValidationResult.Builder().subject(subject).input(value)
                    .explanation(reason).valid(reason.equals("")).build();
        }
    };

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(
            final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description(
                        "Field mapping between schemas. The property name is the field name for the input "
                                + "schema, and the property value is the field name for the output schema. For fields "
                                + "not listed, the processor tries to match names from the input to the output record.")
                .dynamic(true).addValidator(AVRO_FIELDNAME_VALIDATOR).build();
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
    public void onTrigger(ProcessContext context, final ProcessSession session)
            throws ProcessException {
        FlowFile incomingAvro = session.get();
        if (incomingAvro == null) {
            return;
        }

        String inputSchemaProperty = context.getProperty(INPUT_SCHEMA)
                .evaluateAttributeExpressions(incomingAvro).getValue();
        final Schema inputSchema;
        try {
            inputSchema = getSchema(inputSchemaProperty,
                    DefaultConfiguration.get());
        } catch (SchemaNotFoundException e) {
            getLogger().error("Cannot find schema: " + inputSchemaProperty);
            session.transfer(incomingAvro, FAILURE);
            return;
        }
        String outputSchemaProperty = context.getProperty(OUTPUT_SCHEMA)
                .evaluateAttributeExpressions(incomingAvro).getValue();
        final Schema outputSchema;
        try {
            outputSchema = getSchema(outputSchemaProperty,
                    DefaultConfiguration.get());
        } catch (SchemaNotFoundException e) {
            getLogger().error("Cannot find schema: " + outputSchemaProperty);
            session.transfer(incomingAvro, FAILURE);
            return;
        }
        final Map<String, String> fieldMapping = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context
                .getProperties().entrySet()) {
            if (entry.getKey().isDynamic()) {
                fieldMapping.put(entry.getKey().getName(), entry.getValue());
            }
        }
        // Set locale
        final String localeProperty = context.getProperty(LOCALE).getValue();
        final Locale locale = localeProperty.equals(DEFAULT_LOCALE_VALUE) ? Locale.getDefault() : LocaleUtils.toLocale(localeProperty);
        final AvroRecordConverter converter = new AvroRecordConverter(
                inputSchema, outputSchema, fieldMapping, locale);

        final DataFileWriter<Record> writer = new DataFileWriter<>(
                AvroUtil.newDatumWriter(outputSchema, Record.class));
        writer.setCodec(getCodecFactory(context.getProperty(COMPRESSION_TYPE).getValue()));

        final DataFileWriter<Record> failureWriter = new DataFileWriter<>(
                AvroUtil.newDatumWriter(outputSchema, Record.class));
        failureWriter.setCodec(getCodecFactory(context.getProperty(COMPRESSION_TYPE).getValue()));

        try {
            final AtomicLong written = new AtomicLong(0L);
            final FailureTracker failures = new FailureTracker();

            final List<Record> badRecords = Lists.newLinkedList();
            FlowFile incomingAvroCopy = session.clone(incomingAvro);
            FlowFile outgoingAvro = session.write(incomingAvro,
                    new StreamCallback() {
                        @Override
                        public void process(InputStream in, OutputStream out)
                                throws IOException {
                            try (DataFileStream<Record> stream = new DataFileStream<Record>(
                                    in, new GenericDatumReader<Record>(
                                            converter.getInputSchema()))) {
                                try (DataFileWriter<Record> w = writer.create(
                                        outputSchema, out)) {
                                    for (Record record : stream) {
                                        try {
                                            Record converted = converter
                                                    .convert(record);
                                            w.append(converted);
                                            written.incrementAndGet();
                                        } catch (AvroConversionException e) {
                                            failures.add(e);
                                            getLogger().error(
                                                    "Error converting data: "
                                                            + e.getMessage());
                                            badRecords.add(record);
                                        }
                                    }
                                }
                            }
                        }
                    });

            FlowFile badOutput = session.write(incomingAvroCopy,
                    new StreamCallback() {
                        @Override
                        public void process(InputStream in, OutputStream out)
                                throws IOException {

                            try (DataFileWriter<Record> w = failureWriter
                                    .create(inputSchema, out)) {
                                for (Record record : badRecords) {
                                    w.append(record);
                                }
                            }

                        }
                    });

            long errors = failures.count();

            // update only if file transfer is successful
            session.adjustCounter("Converted records", written.get(), false);
            // update only if file transfer is successful
            session.adjustCounter("Conversion errors", errors, false);

            if (written.get() > 0L) {
                outgoingAvro = session.putAttribute(outgoingAvro, CoreAttributes.MIME_TYPE.key(), InferAvroSchema.AVRO_MIME_TYPE);
                session.transfer(outgoingAvro, SUCCESS);
            } else {
                session.remove(outgoingAvro);

                if (errors == 0L) {
                    badOutput = session.putAttribute(badOutput, "errors",
                            "No incoming records");
                    session.transfer(badOutput, FAILURE);
                }
            }

            if (errors > 0L) {
                getLogger().warn(
                        "Failed to convert {}/{} records between Avro Schemas",
                        new Object[] { errors, errors + written.get() });
                badOutput = session.putAttribute(badOutput, "errors",
                        failures.summary());
                session.transfer(badOutput, FAILURE);
            } else {
                session.remove(badOutput);
            }
        } catch (ProcessException | DatasetIOException e) {
            getLogger().error("Failed reading or writing", e);
            session.transfer(incomingAvro, FAILURE);
        } catch (DatasetException e) {
            getLogger().error("Failed to read FlowFile", e);
            session.transfer(incomingAvro, FAILURE);
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                getLogger().warn("Unable to close writer ressource", e);
            }
            try {
                failureWriter.close();
            } catch (IOException e) {
                getLogger().warn("Unable to close writer ressource", e);
            }
        }
    }
}
