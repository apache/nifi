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

import static org.apache.nifi.processor.util.StandardValidators.createLongValidator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.kitesdk.data.DatasetException;
import org.kitesdk.data.DatasetIOException;
import org.kitesdk.data.DatasetRecordException;
import org.kitesdk.data.SchemaNotFoundException;
import org.kitesdk.data.spi.DefaultConfiguration;
import org.kitesdk.data.spi.filesystem.CSVFileReader;
import org.kitesdk.data.spi.filesystem.CSVProperties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.concurrent.atomic.AtomicLong;

@Tags({"kite", "csv", "avro"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("Converts CSV files to Avro according to an Avro Schema")
public class ConvertCSVToAvro extends AbstractKiteConvertProcessor {

    private static final CSVProperties DEFAULTS = new CSVProperties.Builder().build();

    private static final Validator CHAR_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            // Allows special, escaped characters as input, which is then unescaped and converted to a single character.
            // Examples for special characters: \t (or \u0009), \f.
            input = unescapeString(input);

            return new ValidationResult.Builder()
                .subject(subject)
                .input(input)
                .explanation("Only non-null single characters are supported")
                .valid((input.length() == 1 && input.charAt(0) != 0) || context.isExpressionLanguagePresent(input))
                .build();
        }
    };

    private static final Relationship SUCCESS = new Relationship.Builder()
        .name("success")
        .description("Avro content that was converted successfully from CSV")
        .build();

    private static final Relationship FAILURE = new Relationship.Builder()
        .name("failure")
        .description("CSV content that could not be processed")
        .build();

    private static final Relationship INCOMPATIBLE = new Relationship.Builder()
        .name("incompatible")
        .description("CSV content that could not be converted")
        .build();

    @VisibleForTesting
    static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
        .name("Record schema")
        .description("Outgoing Avro schema for each record created from a CSV row")
        .addValidator(SCHEMA_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();

    @VisibleForTesting
    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
        .name("CSV charset")
        .description("Character set for CSV files")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(DEFAULTS.charset)
        .build();

    @VisibleForTesting
    static final PropertyDescriptor DELIMITER = new PropertyDescriptor.Builder()
        .name("CSV delimiter")
        .description("Delimiter character for CSV records")
        .addValidator(CHAR_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(DEFAULTS.delimiter)
        .build();

    @VisibleForTesting
    static final PropertyDescriptor QUOTE = new PropertyDescriptor.Builder()
        .name("CSV quote character")
        .description("Quote character for CSV values")
        .addValidator(CHAR_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(DEFAULTS.quote)
        .build();

    @VisibleForTesting
    static final PropertyDescriptor ESCAPE = new PropertyDescriptor.Builder()
        .name("CSV escape character")
        .description("Escape character for CSV values")
        .addValidator(CHAR_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(DEFAULTS.escape)
        .build();

    @VisibleForTesting
    static final PropertyDescriptor HAS_HEADER = new PropertyDescriptor.Builder()
        .name("Use CSV header line")
        .description("Whether to use the first line as a header")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(String.valueOf(DEFAULTS.useHeader))
        .build();

    @VisibleForTesting
    static final PropertyDescriptor LINES_TO_SKIP = new PropertyDescriptor.Builder()
        .name("Lines to skip")
        .description("Number of lines to skip before reading header or data")
        .addValidator(createLongValidator(0L, Integer.MAX_VALUE, true))
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(String.valueOf(DEFAULTS.linesToSkip))
        .build();

    private static final List<PropertyDescriptor> PROPERTIES = ImmutableList.<PropertyDescriptor> builder()
        .addAll(AbstractKiteProcessor.getProperties())
        .add(SCHEMA)
        .add(CHARSET)
        .add(DELIMITER)
        .add(QUOTE)
        .add(ESCAPE)
        .add(HAS_HEADER)
        .add(LINES_TO_SKIP)
        .add(COMPRESSION_TYPE)
        .build();

    private static final Set<Relationship> RELATIONSHIPS = ImmutableSet.<Relationship> builder()
        .add(SUCCESS)
        .add(FAILURE)
        .add(INCOMPATIBLE)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void createCSVProperties(ProcessContext context) throws IOException {
        super.setDefaultConfiguration(context);
    }

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session)
        throws ProcessException {
        FlowFile incomingCSV = session.get();
        if (incomingCSV == null) {
            return;
        }

        CSVProperties props = new CSVProperties.Builder()
                .charset(context.getProperty(CHARSET).evaluateAttributeExpressions(incomingCSV).getValue())
                .delimiter(context.getProperty(DELIMITER).evaluateAttributeExpressions(incomingCSV).getValue())
                .quote(context.getProperty(QUOTE).evaluateAttributeExpressions(incomingCSV).getValue())
                .escape(context.getProperty(ESCAPE).evaluateAttributeExpressions(incomingCSV).getValue())
                .hasHeader(context.getProperty(HAS_HEADER).evaluateAttributeExpressions(incomingCSV).asBoolean())
                .linesToSkip(context.getProperty(LINES_TO_SKIP).evaluateAttributeExpressions(incomingCSV).asInteger())
                .build();

        String schemaProperty = context.getProperty(SCHEMA)
            .evaluateAttributeExpressions(incomingCSV)
            .getValue();
        final Schema schema;
        try {
            schema = getSchema(schemaProperty, DefaultConfiguration.get());
        } catch (SchemaNotFoundException e) {
            getLogger().error("Cannot find schema: " + schemaProperty);
            session.transfer(incomingCSV, FAILURE);
            return;
        }

        try (final DataFileWriter<Record> writer = new DataFileWriter<>(AvroUtil.newDatumWriter(schema, Record.class))) {
            writer.setCodec(getCodecFactory(context.getProperty(COMPRESSION_TYPE).getValue()));

            try {
                final AtomicLong written = new AtomicLong(0L);
                final FailureTracker failures = new FailureTracker();

                FlowFile badRecords = session.clone(incomingCSV);
                FlowFile outgoingAvro = session.write(incomingCSV, new StreamCallback() {
                    @Override
                    public void process(InputStream in, OutputStream out) throws IOException {
                        try (CSVFileReader<Record> reader = new CSVFileReader<>(
                            in, props, schema, Record.class)) {
                            reader.initialize();
                            try (DataFileWriter<Record> w = writer.create(schema, out)) {
                                while (reader.hasNext()) {
                                    try {
                                        Record record = reader.next();
                                        w.append(record);
                                        written.incrementAndGet();
                                    } catch (DatasetRecordException e) {
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
                        getLogger().warn("Failed to convert {}/{} records from CSV to Avro",
                            new Object[] {errors, errors + written.get()});
                        badRecords = session.putAttribute(
                            badRecords, "errors", failures.summary());
                        session.transfer(badRecords, INCOMPATIBLE);
                    } else {
                        session.remove(badRecords);
                    }

                } else {
                    session.remove(outgoingAvro);

                    if (errors > 0L) {
                        getLogger().warn("Failed to convert {}/{} records from CSV to Avro",
                            new Object[] {errors, errors});
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
                session.transfer(incomingCSV, FAILURE);
            } catch (DatasetException e) {
                getLogger().error("Failed to read FlowFile", e);
                session.transfer(incomingCSV, FAILURE);
            }
        } catch (final IOException ioe) {
            throw new RuntimeException("Unable to close Avro Writer", ioe);
        }
    }

    private static String unescapeString(String input) {
        if (input.length() > 1) {
            input = StringEscapeUtils.unescapeJava(input);
        }
        return input;
    }
}
