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

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
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
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.kitesdk.data.spi.JsonUtil;
import org.kitesdk.data.spi.filesystem.CSVProperties;
import org.kitesdk.data.spi.filesystem.CSVUtil;

import java.io.InputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

@Tags({"kite", "avro", "infer", "schema", "csv", "json"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("Examines the contents of the incoming FlowFile to infer an Avro schema. The processor will" +
        " use the Kite SDK to make an attempt to automatically generate an Avro schema from the incoming content." +
        " When inferring the schema from JSON data the key names will be used in the resulting Avro schema" +
        " definition. When inferring from CSV data a \"header definition\" must be present either as the first line of the incoming data" +
        " or the \"header definition\" must be explicitly set in the property \"CSV Header Definition\". A \"header definition\"" +
        " is simply a single comma separated line defining the names of each column. The \"header definition\" is" +
        " required in order to determine the names that should be given to each field in the resulting Avro definition." +
        " When inferring data types the higher order data type is always used if there is ambiguity." +
        " For example when examining numerical values the type may be set to \"long\" instead of \"integer\" since a long can" +
        " safely hold the value of any \"integer\". Only CSV and JSON content is currently supported for automatically inferring an" +
        " Avro schema. The type of content present in the incoming FlowFile is set by using the property \"Input Content Type\"." +
        " The property can either be explicitly set to CSV, JSON, or \"use mime.type value\" which will examine the" +
        " value of the mime.type attribute on the incoming FlowFile to determine the type of content present.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "mime.type", description = "If configured by property \"Input Content Type\" will" +
                " use this value to determine what sort of content should be inferred from the incoming FlowFile content."),
})
@WritesAttributes({
        @WritesAttribute(attribute = "inferred.avro.schema", description = "If configured by \"Schema output destination\" to" +
                " write to an attribute this will hold the resulting Avro schema from inferring the incoming FlowFile content."),
})
public class InferAvroSchema
        extends AbstractKiteProcessor {

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
                .valid(input.length() == 1 && input.charAt(0) != 0 || context.isExpressionLanguagePresent(input))
                .build();
        }
    };

    public static final String USE_MIME_TYPE = "use mime.type value";
    public static final String JSON_CONTENT = "json";
    public static final String CSV_CONTENT = "csv";

    public static final String AVRO_SCHEMA_ATTRIBUTE_NAME = "inferred.avro.schema";
    public static final String DESTINATION_ATTRIBUTE = "flowfile-attribute";
    public static final String DESTINATION_CONTENT = "flowfile-content";
    public static final String JSON_MIME_TYPE = "application/json";
    public static final String CSV_MIME_TYPE = "text/csv";
    public static final String AVRO_MIME_TYPE = "application/avro-binary";
    public static final String AVRO_FILE_EXTENSION = ".avro";
    public static final Pattern AVRO_RECORD_NAME_PATTERN = Pattern.compile("[A-Za-z_]+[A-Za-z0-9_.]*[^.]");

    public static final PropertyDescriptor SCHEMA_DESTINATION = new PropertyDescriptor.Builder()
            .name("Schema Output Destination")
            .description("Control if Avro schema is written as a new flowfile attribute '" + AVRO_SCHEMA_ATTRIBUTE_NAME + "' " +
                    "or written in the flowfile content. Writing to flowfile content will overwrite any " +
                    "existing flowfile content.")
            .required(true)
            .allowableValues(DESTINATION_ATTRIBUTE, DESTINATION_CONTENT)
            .defaultValue(DESTINATION_CONTENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INPUT_CONTENT_TYPE = new PropertyDescriptor.Builder()
            .name("Input Content Type")
            .description("Content Type of data present in the incoming FlowFile's content. Only \"" +
                    JSON_CONTENT + "\" or \"" + CSV_CONTENT + "\" are supported." +
                    " If this value is set to \"" + USE_MIME_TYPE + "\" the incoming Flowfile's attribute \"" + CoreAttributes.MIME_TYPE + "\"" +
                    " will be used to determine the Content Type.")
            .allowableValues(USE_MIME_TYPE, JSON_CONTENT, CSV_CONTENT)
            .defaultValue(USE_MIME_TYPE)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor GET_CSV_HEADER_DEFINITION_FROM_INPUT = new PropertyDescriptor.Builder()
            .name("Get CSV Header Definition From Data")
            .description("This property only applies to CSV content type. If \"true\" the processor will attempt to read the CSV header definition from the" +
                    " first line of the input data.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor CSV_HEADER_DEFINITION = new PropertyDescriptor.Builder()
            .name("CSV Header Definition")
            .description("This property only applies to CSV content type. Comma separated string defining the column names expected in the CSV data." +
                    " EX: \"fname,lname,zip,address\". The elements present in this string should be in the same order" +
                    " as the underlying data. Setting this property will cause the value of" +
                    " \"" + GET_CSV_HEADER_DEFINITION_FROM_INPUT.getName() + "\" to be ignored instead using this value.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    public static final PropertyDescriptor HEADER_LINE_SKIP_COUNT = new PropertyDescriptor.Builder()
            .name("CSV Header Line Skip Count")
            .description("This property only applies to CSV content type. Specifies the number of lines that should be skipped when reading the CSV data." +
                    " Setting this value to 0 is equivalent to saying \"the entire contents of the file should be read\". If the" +
                    " property \"" + GET_CSV_HEADER_DEFINITION_FROM_INPUT.getName() + "\" is set then the first line of the CSV " +
                    " file will be read in and treated as the CSV header definition. Since this will remove the header line from the data" +
                    " care should be taken to make sure the value of \"CSV header Line Skip Count\" is set to 0 to ensure" +
                    " no data is skipped.")
            .required(true)
            .defaultValue("0")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor DELIMITER = new PropertyDescriptor.Builder()
            .name("CSV delimiter")
            .description("Delimiter character for CSV records")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(CHAR_VALIDATOR)
            .defaultValue(",")
            .build();

    public static final PropertyDescriptor ESCAPE_STRING = new PropertyDescriptor.Builder()
            .name("CSV Escape String")
            .description("This property only applies to CSV content type. String that represents an escape sequence" +
                    " in the CSV FlowFile content data.")
            .required(true)
            .defaultValue("\\")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUOTE_STRING = new PropertyDescriptor.Builder()
            .name("CSV Quote String")
            .description("This property only applies to CSV content type. String that represents a literal quote" +
                    " character in the CSV FlowFile content data.")
            .required(true)
            .defaultValue("'")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor RECORD_NAME = new PropertyDescriptor.Builder()
            .name("Avro Record Name")
            .description("Value to be placed in the Avro record schema \"name\" field. The value must adhere to the Avro naming "
                    + "rules for fullname. If Expression Language is present then the evaluated value must adhere to the Avro naming rules.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.createRegexMatchingValidator(AVRO_RECORD_NAME_PATTERN))
            .build();

    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Charset")
            .description("Character encoding of CSV data.")
            .required(true)
            .defaultValue("UTF-8")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRETTY_AVRO_OUTPUT = new PropertyDescriptor.Builder()
            .name("Pretty Avro Output")
            .description("If true the Avro output will be formatted.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor NUM_RECORDS_TO_ANALYZE = new PropertyDescriptor.Builder()
            .name("Number Of Records To Analyze")
            .description("This property only applies to JSON content type. The number of JSON records that should be" +
                    " examined to determine the Avro schema. The higher the value the better chance kite has of detecting" +
                    " the appropriate type. However the default value of 10 is almost always enough.")
            .required(true)
            .defaultValue("10")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();


    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully created Avro schema from data.").build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
            .description("Original incoming FlowFile data").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Failed to create Avro schema from data.").build();

    public static final Relationship REL_UNSUPPORTED_CONTENT = new Relationship.Builder().name("unsupported content")
            .description("The content found in the flowfile content is not of the required format.").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCHEMA_DESTINATION);
        properties.add(INPUT_CONTENT_TYPE);
        properties.add(CSV_HEADER_DEFINITION);
        properties.add(GET_CSV_HEADER_DEFINITION_FROM_INPUT);
        properties.add(HEADER_LINE_SKIP_COUNT);
        properties.add(DELIMITER);
        properties.add(ESCAPE_STRING);
        properties.add(QUOTE_STRING);
        properties.add(PRETTY_AVRO_OUTPUT);
        properties.add(RECORD_NAME);
        properties.add(NUM_RECORDS_TO_ANALYZE);
        properties.add(CHARSET);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        relationships.add(REL_UNSUPPORTED_CONTENT);
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile original = session.get();
        if (original == null) {
            return;
        }

        try {

            final AtomicReference<String> avroSchema = new AtomicReference<>();
            switch (context.getProperty(INPUT_CONTENT_TYPE).getValue()) {
                case USE_MIME_TYPE:
                    avroSchema.set(inferAvroSchemaFromMimeType(original, context, session));
                    break;
                case JSON_CONTENT:
                    avroSchema.set(inferAvroSchemaFromJSON(original, context, session));
                    break;
                case CSV_CONTENT:
                    avroSchema.set(inferAvroSchemaFromCSV(original, context, session));
                    break;
                default:
                    //Shouldn't be possible but just in case
                    session.transfer(original, REL_UNSUPPORTED_CONTENT);
                    break;
            }


            if (StringUtils.isNotEmpty(avroSchema.get())) {

                String destination = context.getProperty(SCHEMA_DESTINATION).getValue();
                FlowFile avroSchemaFF = null;

                switch (destination) {
                    case DESTINATION_ATTRIBUTE:
                        avroSchemaFF = session.putAttribute(session.clone(original), AVRO_SCHEMA_ATTRIBUTE_NAME, avroSchema.get());
                        //Leaves the original CoreAttributes.MIME_TYPE in place.
                        break;
                    case DESTINATION_CONTENT:
                        avroSchemaFF = session.write(session.create(), new OutputStreamCallback() {
                            @Override
                            public void process(OutputStream out) throws IOException {
                                out.write(avroSchema.get().getBytes());
                            }
                        });
                        avroSchemaFF = session.putAttribute(avroSchemaFF, CoreAttributes.MIME_TYPE.key(), AVRO_MIME_TYPE);
                        break;
                    default:
                        break;
                }

                //Transfer the sessions.
                avroSchemaFF = session.putAttribute(avroSchemaFF, CoreAttributes.FILENAME.key(), (original.getAttribute(CoreAttributes.FILENAME.key()) + AVRO_FILE_EXTENSION));
                session.transfer(avroSchemaFF, REL_SUCCESS);
                session.transfer(original, REL_ORIGINAL);
            } else {
                //If the avroSchema is null then the content type is unknown and therefore unsupported
                session.transfer(original, REL_UNSUPPORTED_CONTENT);
            }

        } catch (Exception ex) {
            getLogger().error("Failed to infer Avro schema for {} due to {}", new Object[] {original, ex});
            session.transfer(original, REL_FAILURE);
        }
    }


    /**
     * Infers the Avro schema from the input Flowfile content. To infer an Avro schema for CSV content a header line is
     * required. You can configure the processor to pull that header line from the first line of the CSV data if it is
     * present OR you can manually supply the desired header line as a property value.
     *
     * @param inputFlowFile
     *  The original input FlowFile containing the CSV content as it entered this processor.
     *
     * @param context
     *  ProcessContext to pull processor configurations.
     *
     * @param session
     *  ProcessSession to transfer FlowFiles
     */
    private String inferAvroSchemaFromCSV(final FlowFile inputFlowFile, final ProcessContext context, final ProcessSession session) {

        //Determines the header line either from the property input or the first line of the delimited file.
        final AtomicReference<String> header = new AtomicReference<>();
        final AtomicReference<Boolean> hasHeader = new AtomicReference<>();

        if (context.getProperty(GET_CSV_HEADER_DEFINITION_FROM_INPUT).asBoolean() == Boolean.TRUE) {
            //Read the first line of the file to get the header value.
            session.read(inputFlowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    BufferedReader br = new BufferedReader(new InputStreamReader(in));
                    header.set(br.readLine());
                    hasHeader.set(Boolean.TRUE);
                    br.close();
                }
            });
            hasHeader.set(Boolean.TRUE);
        } else {
            header.set(context.getProperty(CSV_HEADER_DEFINITION).evaluateAttributeExpressions(inputFlowFile).getValue());
            hasHeader.set(Boolean.FALSE);
        }

        //Prepares the CSVProperties for kite
        CSVProperties props = new CSVProperties.Builder()
                .charset(context.getProperty(CHARSET).evaluateAttributeExpressions(inputFlowFile).getValue())
                .delimiter(context.getProperty(DELIMITER).evaluateAttributeExpressions(inputFlowFile).getValue())
                .quote(context.getProperty(QUOTE_STRING).evaluateAttributeExpressions(inputFlowFile).getValue())
                .escape(context.getProperty(ESCAPE_STRING).evaluateAttributeExpressions(inputFlowFile).getValue())
                .linesToSkip(context.getProperty(HEADER_LINE_SKIP_COUNT).evaluateAttributeExpressions(inputFlowFile).asInteger())
                .header(header.get())
                .hasHeader(hasHeader.get())
                .build();

        final AtomicReference<String> avroSchema = new AtomicReference<>();

        session.read(inputFlowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                avroSchema.set(CSVUtil
                        .inferSchema(
                                context.getProperty(RECORD_NAME).evaluateAttributeExpressions(inputFlowFile).getValue(), in, props)
                        .toString(context.getProperty(PRETTY_AVRO_OUTPUT).asBoolean()));
            }
        });

        return avroSchema.get();
    }

    /**
     * Infers the Avro schema from the input Flowfile content.
     *
     * @param inputFlowFile
     *  The original input FlowFile containing the JSON content as it entered this processor.
     *
     * @param context
     *  ProcessContext to pull processor configurations.
     *
     * @param session
     *  ProcessSession to transfer FlowFiles
     */
    private String inferAvroSchemaFromJSON(final FlowFile inputFlowFile, final ProcessContext context, final ProcessSession session) {

        final AtomicReference<String> avroSchema = new AtomicReference<>();
        session.read(inputFlowFile, new InputStreamCallback() {
            @Override
            public void process(InputStream in) throws IOException {
                Schema as = JsonUtil.inferSchema(
                        in, context.getProperty(RECORD_NAME).evaluateAttributeExpressions(inputFlowFile).getValue(),
                        context.getProperty(NUM_RECORDS_TO_ANALYZE).evaluateAttributeExpressions(inputFlowFile).asInteger());
                avroSchema.set(as.toString(context.getProperty(PRETTY_AVRO_OUTPUT).asBoolean()));

            }
        });

        return avroSchema.get();
    }

    /**
     * Examines the incoming FlowFiles mime.type attribute to determine if the schema should be inferred for CSV or JSON data.
     *
     * @param inputFlowFile
     *  The original input FlowFile containing the content.
     *
     * @param context
     *  ProcessContext to pull processor configurations.
     *
     * @param session
     *  ProcessSession to transfer FlowFiles
     */
    private String inferAvroSchemaFromMimeType(final FlowFile inputFlowFile, final ProcessContext context, final ProcessSession session) {

        String mimeType = inputFlowFile.getAttribute(CoreAttributes.MIME_TYPE.key());
        String avroSchema = "";

        if (mimeType!= null) {
            switch (mimeType) {
                case JSON_MIME_TYPE:
                    getLogger().debug("Inferred content type as JSON from \"{}\" value of \"{}\"", new Object[]{CoreAttributes.MIME_TYPE.key(),
                            inputFlowFile.getAttribute(CoreAttributes.MIME_TYPE.key())});
                    avroSchema = inferAvroSchemaFromJSON(inputFlowFile, context, session);
                    break;
                case CSV_MIME_TYPE:
                    getLogger().debug("Inferred content type as CSV from \"{}\" value of \"{}\"", new Object[]{CoreAttributes.MIME_TYPE.key(),
                            inputFlowFile.getAttribute(CoreAttributes.MIME_TYPE.key())});
                    avroSchema = inferAvroSchemaFromCSV(inputFlowFile, context, session);
                    break;
                default:
                    getLogger().warn("Unable to infer Avro Schema from {} because its mime type is {}, " +
                            " which is not supported by this Processor", new Object[] {inputFlowFile, mimeType} );
                    break;
            }
        }

        return avroSchema;
    }

    private static String unescapeString(String input) {
        if (input.length() > 1) {
            input = StringEscapeUtils.unescapeJava(input);
        }
        return input;
    }
}
