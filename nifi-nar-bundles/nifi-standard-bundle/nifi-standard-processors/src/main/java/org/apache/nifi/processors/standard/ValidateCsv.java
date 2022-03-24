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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ParseBigDecimal;
import org.supercsv.cellprocessor.ParseBool;
import org.supercsv.cellprocessor.ParseChar;
import org.supercsv.cellprocessor.ParseDate;
import org.supercsv.cellprocessor.ParseDouble;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.DMinMax;
import org.supercsv.cellprocessor.constraint.Equals;
import org.supercsv.cellprocessor.constraint.ForbidSubStr;
import org.supercsv.cellprocessor.constraint.IsIncludedIn;
import org.supercsv.cellprocessor.constraint.LMinMax;
import org.supercsv.cellprocessor.constraint.NotNull;
import org.supercsv.cellprocessor.constraint.RequireHashCode;
import org.supercsv.cellprocessor.constraint.RequireSubStr;
import org.supercsv.cellprocessor.constraint.StrMinMax;
import org.supercsv.cellprocessor.constraint.StrNotNullOrEmpty;
import org.supercsv.cellprocessor.constraint.StrRegEx;
import org.supercsv.cellprocessor.constraint.Strlen;
import org.supercsv.cellprocessor.constraint.Unique;
import org.supercsv.cellprocessor.constraint.UniqueHashCode;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"csv", "schema", "validation"})
@CapabilityDescription("Validates the contents of FlowFiles against a user-specified CSV schema. " +
        "Take a look at the additional documentation of this processor for some schema examples.")
@WritesAttributes({
    @WritesAttribute(attribute="count.valid.lines", description="If line by line validation, number of valid lines extracted from the source data"),
    @WritesAttribute(attribute="count.invalid.lines", description="If line by line validation, number of invalid lines extracted from the source data"),
    @WritesAttribute(attribute="count.total.lines", description="If line by line validation, total number of lines in the source data"),
    @WritesAttribute(attribute="validation.error.message", description="For flow files routed to invalid, message of the first validation error")
})
public class ValidateCsv extends AbstractProcessor {

    private final static List<String> allowedOperators = Arrays.asList("ParseBigDecimal", "ParseBool", "ParseChar", "ParseDate",
            "ParseDouble", "ParseInt", "ParseLong", "Optional", "DMinMax", "Equals", "ForbidSubStr", "LMinMax", "NotNull", "Null",
            "RequireHashCode", "RequireSubStr", "Strlen", "StrMinMax", "StrNotNullOrEmpty", "StrRegEx", "Unique",
            "UniqueHashCode", "IsIncludedIn");

    private static final String routeWholeFlowFile = "FlowFile validation";
    private static final String routeLinesIndividually = "Line by line validation";

    public static final AllowableValue VALIDATE_WHOLE_FLOWFILE = new AllowableValue(routeWholeFlowFile, routeWholeFlowFile,
            "As soon as an error is found in the CSV file, the validation will stop and the whole flow file will be routed to the 'invalid'"
                    + " relationship. This option offers best performances.");

    public static final AllowableValue VALIDATE_LINES_INDIVIDUALLY = new AllowableValue(routeLinesIndividually, routeLinesIndividually,
            "In case an error is found, the input CSV file will be split into two FlowFiles: one routed to the 'valid' "
                    + "relationship containing all the correct lines and one routed to the 'invalid' relationship containing all "
                    + "the incorrect lines. Take care if choosing this option while using Unique cell processors in schema definition:"
                    + "the first occurrence will be considered valid and the next ones as invalid.");

    public static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
            .name("validate-csv-schema")
            .displayName("Schema")
            .description("The schema to be used for validation. Is expected a comma-delimited string representing the cell "
                    + "processors to apply. The following cell processors are allowed in the schema definition: "
                    + allowedOperators.toString() + ". Note: cell processors cannot be nested except with Optional.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .build();

    public static final PropertyDescriptor HEADER = new PropertyDescriptor.Builder()
            .name("validate-csv-header")
            .displayName("Header")
            .description("True if the incoming flow file contains a header to ignore, false otherwise.")
            .required(true)
            .defaultValue("true")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor QUOTE_CHARACTER = new PropertyDescriptor.Builder()
            .name("validate-csv-quote")
            .displayName("Quote character")
            .description("Character used as 'quote' in the incoming data. Example: \"")
            .required(true)
            .defaultValue("\"")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DELIMITER_CHARACTER = new PropertyDescriptor.Builder()
            .name("validate-csv-delimiter")
            .displayName("Delimiter character")
            .description("Character used as 'delimiter' in the incoming data. Example: ,")
            .required(true)
            .defaultValue(",")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor END_OF_LINE_CHARACTER = new PropertyDescriptor.Builder()
            .name("validate-csv-eol")
            .displayName("End of line symbols")
            .description("Symbols used as 'end of line' in the incoming data. Example: \\n")
            .required(true)
            .defaultValue("\\n")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor VALIDATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("validate-csv-strategy")
            .displayName("Validation strategy")
            .description("Strategy to apply when routing input files to output relationships.")
            .required(true)
            .defaultValue(VALIDATE_WHOLE_FLOWFILE.getValue())
            .allowableValues(VALIDATE_LINES_INDIVIDUALLY, VALIDATE_WHOLE_FLOWFILE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_VALID = new Relationship.Builder()
            .name("valid")
            .description("FlowFiles that are successfully validated against the schema are routed to this relationship")
            .build();
    public static final Relationship REL_INVALID = new Relationship.Builder()
            .name("invalid")
            .description("FlowFiles that are not valid according to the specified schema are routed to this relationship")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SCHEMA);
        properties.add(HEADER);
        properties.add(DELIMITER_CHARACTER);
        properties.add(QUOTE_CHARACTER);
        properties.add(END_OF_LINE_CHARACTER);
        properties.add(VALIDATION_STRATEGY);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_VALID);
        relationships.add(REL_INVALID);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {

        PropertyValue schemaProp = context.getProperty(SCHEMA);
        String schema = schemaProp.getValue();
        String subject = SCHEMA.getName();

        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(schema)) {
            return Collections.singletonList(new ValidationResult.Builder().subject(subject).input(schema).explanation("Expression Language Present").valid(true).build());
        }
        // If no Expression Language is present, try parsing the schema
        try {
            this.parseSchema(schema);
        } catch (Exception e) {
            final List<ValidationResult> problems = new ArrayList<>(1);
            problems.add(new ValidationResult.Builder().subject(subject)
                    .input(schema)
                    .valid(false)
                    .explanation("Error while parsing the schema: " + e.getMessage())
                    .build());
            return problems;
        }
        return super.customValidate(context);
    }

    public CsvPreference getPreference(final ProcessContext context, final FlowFile flowFile) {
        // When going from the UI to Java, the characters are escaped so that what you
        // input is transferred over to Java as is. So when you type the characters "\"
        // and "n" into the UI the Java string will end up being those two characters
        // not the interpreted value "\n".
        final String msgDemarcator = context.getProperty(END_OF_LINE_CHARACTER).evaluateAttributeExpressions(flowFile).getValue().replace("\\n", "\n").replace("\\r", "\r").replace("\\t", "\t");
        return new CsvPreference.Builder(context.getProperty(QUOTE_CHARACTER).evaluateAttributeExpressions(flowFile).getValue().charAt(0),
                context.getProperty(DELIMITER_CHARACTER).evaluateAttributeExpressions(flowFile).getValue().charAt(0), msgDemarcator).build();
    }

    /**
     * Method used to parse the string supplied by the user. The string is converted
     * to a list of cell processors used to validate the CSV data.
     * @param schema Schema to parse
     */
    private CellProcessor[] parseSchema(String schema) {
        List<CellProcessor> processorsList = new ArrayList<>();

        String remaining = schema;
        while(remaining.length() > 0) {
            remaining = setProcessor(remaining, processorsList);
        }

        return processorsList.toArray(new CellProcessor[processorsList.size()]);
    }

    private String setProcessor(String remaining, List<CellProcessor> processorsList) {
        StringBuffer buffer = new StringBuffer();
        String inputString = remaining;
        int i = 0;
        int opening = 0;
        int closing = 0;
        while(buffer.length() != inputString.length()) {
            char c = remaining.charAt(i);
            i++;

            if(opening == 0 && c == ',') {
                if(i == 1) {
                    inputString = inputString.substring(1);
                    continue;
                }
                break;
            }

            buffer.append(c);

            if(c == '(') {
                opening++;
            } else if(c == ')') {
                closing++;
            }

            if(opening > 0 && opening == closing) {
                break;
            }
        }

        final String procString = buffer.toString().trim();
        opening = procString.indexOf('(');
        String method = procString;
        String argument = null;
        if(opening != -1) {
            argument = method.substring(opening + 1, method.length() - 1);
            method = method.substring(0, opening);
        }

        processorsList.add(getProcessor(method.toLowerCase(), argument));

        return remaining.substring(i);
    }

    private CellProcessor getProcessor(String method, String argument) {
        switch (method) {

            case "optional":
                int opening = argument.indexOf('(');
                String subMethod = argument;
                String subArgument = null;
                if(opening != -1) {
                    subArgument = subMethod.substring(opening + 1, subMethod.length() - 1);
                    subMethod = subMethod.substring(0, opening);
                }
                return new Optional(getProcessor(subMethod.toLowerCase(), subArgument));

            case "parsedate":
                return new ParseDate(argument.substring(1, argument.length() - 1));

            case "parsedouble":
                if(argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("ParseDouble does not expect any argument but has " + argument);
                return new ParseDouble();

            case "parsebigdecimal":
                if(argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("ParseBigDecimal does not expect any argument but has " + argument);
                return new ParseBigDecimal();

            case "parsebool":
                if(argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("ParseBool does not expect any argument but has " + argument);
                return new ParseBool();

            case "parsechar":
                if(argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("ParseChar does not expect any argument but has " + argument);
                return new ParseChar();

            case "parseint":
                if(argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("ParseInt does not expect any argument but has " + argument);
                return new ParseInt();

            case "parselong":
                if(argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("ParseLong does not expect any argument but has " + argument);
                return new ParseLong();

            case "notnull":
                if(argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("NotNull does not expect any argument but has " + argument);
                return new NotNull();

            case "strregex":
                return new StrRegEx(argument.substring(1, argument.length() - 1));

            case "unique":
                if(argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("Unique does not expect any argument but has " + argument);
                return new Unique();

            case "uniquehashcode":
                if(argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("UniqueHashCode does not expect any argument but has " + argument);
                return new UniqueHashCode();

            case "strlen":
                String[] splts = argument.split(",");
                int[] requiredLengths = new int[splts.length];
                for(int i = 0; i < splts.length; i++) {
                    requiredLengths[i] = Integer.parseInt(splts[i]);
                }
                return new Strlen(requiredLengths);

            case "strminmax":
                String[] splits = argument.split(",");
                return new StrMinMax(Long.parseLong(splits[0]), Long.parseLong(splits[1]));

            case "lminmax":
                String[] args = argument.split(",");
                return new LMinMax(Long.parseLong(args[0]), Long.parseLong(args[1]));

            case "dminmax":
                String[] doubles = argument.split(",");
                return new DMinMax(Double.parseDouble(doubles[0]), Double.parseDouble(doubles[1]));

            case "equals":
                if(argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("Equals does not expect any argument but has " + argument);
                return new Equals();

            case "forbidsubstr":
                String[] forbiddenSubStrings = argument.replaceAll("\"", "").split(",[ ]*");
                return new ForbidSubStr(forbiddenSubStrings);

            case "requiresubstr":
                String[] requiredSubStrings = argument.replaceAll("\"", "").split(",[ ]*");
                return new RequireSubStr(requiredSubStrings);

            case "strnotnullorempty":
                if(argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("StrNotNullOrEmpty does not expect any argument but has " + argument);
                return new StrNotNullOrEmpty();

            case "requirehashcode":
                String[] hashs = argument.split(",");
                int[] hashcodes = new int[hashs.length];
                for(int i = 0; i < hashs.length; i++) {
                    hashcodes[i] = Integer.parseInt(hashs[i]);
                }
                return new RequireHashCode(hashcodes);

            case "null":
                if(argument != null && !argument.isEmpty())
                    throw new IllegalArgumentException("Null does not expect any argument but has " + argument);
                return null;

            case "isincludedin":
                String[] elements = argument.replaceAll("\"", "").split(",[ ]*");
                return new IsIncludedIn(elements);

            default:
                throw new IllegalArgumentException("[" + method + "] is not an allowed method to define a Cell Processor");
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final CsvPreference csvPref = getPreference(context, flowFile);
        final boolean header = context.getProperty(HEADER).asBoolean();
        final ComponentLog logger = getLogger();
        final String schema = context.getProperty(SCHEMA).evaluateAttributeExpressions(flowFile).getValue();
        final CellProcessor[] cellProcs = this.parseSchema(schema);
        final boolean isWholeFFValidation = context.getProperty(VALIDATION_STRATEGY).getValue().equals(VALIDATE_WHOLE_FLOWFILE.getValue());

        final AtomicReference<Boolean> valid = new AtomicReference<Boolean>(true);
        final AtomicReference<Boolean> isFirstLineValid = new AtomicReference<Boolean>(true);
        final AtomicReference<Boolean> isFirstLineInvalid = new AtomicReference<Boolean>(true);
        final AtomicReference<Integer> okCount = new AtomicReference<Integer>(0);
        final AtomicReference<Integer> totalCount = new AtomicReference<Integer>(0);
        final AtomicReference<FlowFile> invalidFF = new AtomicReference<FlowFile>(null);
        final AtomicReference<FlowFile> validFF = new AtomicReference<FlowFile>(null);
        final AtomicReference<String> validationError = new AtomicReference<String>(null);

        if(!isWholeFFValidation) {
            invalidFF.set(session.create(flowFile));
            validFF.set(session.create(flowFile));
        }

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream in) throws IOException {
                try(final NifiCsvListReader listReader = new NifiCsvListReader(new InputStreamReader(in), csvPref)) {

                    // handling of header
                    if(header) {

                        // read header
                        listReader.read();

                        if(!isWholeFFValidation) {
                            invalidFF.set(session.append(invalidFF.get(), new OutputStreamCallback() {
                                @Override
                                public void process(OutputStream out) throws IOException {
                                    out.write(print(listReader.getUntokenizedRow(), csvPref, true));
                                }
                            }));
                            validFF.set(session.append(validFF.get(), new OutputStreamCallback() {
                                @Override
                                public void process(OutputStream out) throws IOException {
                                    out.write(print(listReader.getUntokenizedRow(), csvPref, true));
                                }
                            }));
                            isFirstLineValid.set(false);
                            isFirstLineInvalid.set(false);
                        }
                    }

                    boolean stop = false;

                    while (!stop) {
                        try {

                            // read next row and check if no more row
                            stop = listReader.read(cellProcs) == null;

                            if(!isWholeFFValidation && !stop) {
                                validFF.set(session.append(validFF.get(), new OutputStreamCallback() {
                                    @Override
                                    public void process(OutputStream out) throws IOException {
                                        out.write(print(listReader.getUntokenizedRow(), csvPref, isFirstLineValid.get()));
                                    }
                                }));
                                okCount.set(okCount.get() + 1);

                                if(isFirstLineValid.get()) {
                                    isFirstLineValid.set(false);
                                }
                            }

                        } catch (final SuperCsvException e) {
                            valid.set(false);
                            if(isWholeFFValidation) {
                                validationError.set(e.getLocalizedMessage());
                                logger.debug("Failed to validate {} against schema due to {}; routing to 'invalid'", new Object[]{flowFile}, e);
                                break;
                            } else {
                                // we append the invalid line to the flow file that will be routed to invalid relationship
                                invalidFF.set(session.append(invalidFF.get(), new OutputStreamCallback() {
                                    @Override
                                    public void process(OutputStream out) throws IOException {
                                        out.write(print(listReader.getUntokenizedRow(), csvPref, isFirstLineInvalid.get()));
                                    }
                                }));

                                if(isFirstLineInvalid.get()) {
                                    isFirstLineInvalid.set(false);
                                }

                                if(validationError.get() == null) {
                                    validationError.set(e.getLocalizedMessage());
                                }
                            }
                        } finally {
                            if(!isWholeFFValidation) {
                                totalCount.set(totalCount.get() + 1);
                            }
                        }
                    }

                } catch (final IOException e) {
                    valid.set(false);
                    logger.error("Failed to validate {} against schema due to {}", new Object[]{flowFile}, e);
                }
            }
        });

        if(isWholeFFValidation) {
            if (valid.get()) {
                logger.debug("Successfully validated {} against schema; routing to 'valid'", new Object[]{flowFile});
                session.getProvenanceReporter().route(flowFile, REL_VALID);
                session.transfer(flowFile, REL_VALID);
            } else {
                session.getProvenanceReporter().route(flowFile, REL_INVALID);
                session.putAttribute(flowFile, "validation.error.message", validationError.get());
                session.transfer(flowFile, REL_INVALID);
            }
        } else {
            if (valid.get()) {
                logger.debug("Successfully validated {} against schema; routing to 'valid'", new Object[]{validFF.get()});
                session.getProvenanceReporter().route(validFF.get(), REL_VALID, "All " + totalCount.get() + " line(s) are valid");
                session.putAttribute(validFF.get(), "count.valid.lines", Integer.toString(totalCount.get()));
                session.putAttribute(validFF.get(), "count.total.lines", Integer.toString(totalCount.get()));
                session.transfer(validFF.get(), REL_VALID);
                session.remove(invalidFF.get());
                session.remove(flowFile);
            } else if (okCount.get() != 0) {
                // because of the finally within the 'while' loop
                totalCount.set(totalCount.get() - 1);

                logger.debug("Successfully validated {}/{} line(s) in {} against schema; routing valid lines to 'valid' and invalid lines to 'invalid'",
                        new Object[]{okCount.get(), totalCount.get(), flowFile});
                session.getProvenanceReporter().route(validFF.get(), REL_VALID, okCount.get() + " valid line(s)");
                session.putAttribute(validFF.get(), "count.total.lines", Integer.toString(totalCount.get()));
                session.putAttribute(validFF.get(), "count.valid.lines", Integer.toString(okCount.get()));
                session.transfer(validFF.get(), REL_VALID);
                session.getProvenanceReporter().route(invalidFF.get(), REL_INVALID, (totalCount.get() - okCount.get()) + " invalid line(s)");
                session.putAttribute(invalidFF.get(), "count.invalid.lines", Integer.toString((totalCount.get() - okCount.get())));
                session.putAttribute(invalidFF.get(), "count.total.lines", Integer.toString(totalCount.get()));
                session.putAttribute(invalidFF.get(), "validation.error.message", validationError.get());
                session.transfer(invalidFF.get(), REL_INVALID);
                session.remove(flowFile);
            } else {
                logger.debug("All lines in {} are invalid; routing to 'invalid'", new Object[]{invalidFF.get()});
                session.getProvenanceReporter().route(invalidFF.get(), REL_INVALID, "All " + totalCount.get() + " line(s) are invalid");
                session.putAttribute(invalidFF.get(), "count.invalid.lines", Integer.toString(totalCount.get()));
                session.putAttribute(invalidFF.get(), "count.total.lines", Integer.toString(totalCount.get()));
                session.putAttribute(invalidFF.get(), "validation.error.message", validationError.get());
                session.transfer(invalidFF.get(), REL_INVALID);
                session.remove(validFF.get());
                session.remove(flowFile);
            }
        }
    }

    private byte[] print(String row, CsvPreference csvPref, boolean isFirstLine) {
        StringBuffer buffer = new StringBuffer();
        if (!isFirstLine) {
            buffer.append(csvPref.getEndOfLineSymbols());
        }
        return buffer.append(row).toString().getBytes();
    }

    /**
     * This is required to avoid the side effect of Parse* cell processors. If not overriding
     * this method, parsing will return objects and writing objects could result in a different
     * output in comparison to the input.
     */
    private class NifiCsvListReader extends CsvListReader {

        public NifiCsvListReader(Reader reader, CsvPreference preferences) {
            super(reader, preferences);
        }

        @Override
        public List<Object> read(CellProcessor... processors) throws IOException {
            if( processors == null ) {
                throw new NullPointerException("Processors should not be null");
            }
            if( readRow() ) {
                super.executeProcessors(new ArrayList<Object>(getColumns().size()), processors);
                return new ArrayList<Object>(getColumns());
            }
            return null; // EOF
        }

    }

}
