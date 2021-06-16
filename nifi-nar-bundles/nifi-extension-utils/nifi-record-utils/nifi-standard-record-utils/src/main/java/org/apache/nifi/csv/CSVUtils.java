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

package org.apache.nifi.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CSVUtils {

    private static final Logger LOG = LoggerFactory.getLogger(CSVUtils.class);

    public static final AllowableValue CUSTOM = new AllowableValue("custom", "Custom Format",
        "The format of the CSV is configured by using the properties of this Controller Service, such as Value Separator");
    public static final AllowableValue RFC_4180 = new AllowableValue("rfc-4180", "RFC 4180", "CSV data follows the RFC 4180 Specification defined at https://tools.ietf.org/html/rfc4180");
    public static final AllowableValue EXCEL = new AllowableValue("excel", "Microsoft Excel", "CSV data follows the format used by Microsoft Excel");
    public static final AllowableValue TDF = new AllowableValue("tdf", "Tab-Delimited", "CSV data is Tab-Delimited instead of Comma Delimited");
    public static final AllowableValue INFORMIX_UNLOAD = new AllowableValue("informix-unload", "Informix Unload", "The format used by Informix when issuing the UNLOAD TO file_name command");
    public static final AllowableValue INFORMIX_UNLOAD_CSV = new AllowableValue("informix-unload-csv", "Informix Unload Escape Disabled",
        "The format used by Informix when issuing the UNLOAD TO file_name command with escaping disabled");
    public static final AllowableValue MYSQL = new AllowableValue("mysql", "MySQL Format", "CSV data follows the format used by MySQL");

    public static final PropertyDescriptor CSV_FORMAT = new PropertyDescriptor.Builder()
        .name("CSV Format")
        .description("Specifies which \"format\" the CSV data is in, or specifies if custom formatting should be used.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues(CUSTOM, RFC_4180, EXCEL, TDF, MYSQL, INFORMIX_UNLOAD, INFORMIX_UNLOAD_CSV)
        .defaultValue(CUSTOM.getValue())
        .required(true)
        .build();
    public static final PropertyDescriptor VALUE_SEPARATOR = new PropertyDescriptor.Builder()
        .name("Value Separator")
        .description("The character that is used to separate values/fields in a CSV Record. If the property has been specified via Expression Language " +
                "but the expression gets evaluated to an invalid Value Separator at runtime, then it will be skipped and the default Value Separator will be used.")
        .addValidator(CSVValidators.UNESCAPED_SINGLE_CHAR_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(",")
        .required(true)
        .build();
    public static final PropertyDescriptor QUOTE_CHAR = new PropertyDescriptor.Builder()
        .name("Quote Character")
        .description("The character that is used to quote values so that escape characters do not have to be used. If the property has been specified via Expression Language " +
                "but the expression gets evaluated to an invalid Quote Character at runtime, then it will be skipped and the default Quote Character will be used.")
        .addValidator(new CSVValidators.SingleCharacterValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("\"")
        .required(true)
        .build();
    public static final PropertyDescriptor FIRST_LINE_IS_HEADER = new PropertyDescriptor.Builder()
        .name("Skip Header Line")
        .displayName("Treat First Line as Header")
        .description("Specifies whether or not the first line of CSV should be considered a Header or should be considered a record. If the Schema Access Strategy "
            + "indicates that the columns must be defined in the header, then this property will be ignored, since the header must always be "
            + "present and won't be processed as a Record. Otherwise, if 'true', then the first line of CSV data will not be processed as a record and if 'false',"
            + "then the first line will be interpreted as a record.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();
    public static final PropertyDescriptor IGNORE_CSV_HEADER = new PropertyDescriptor.Builder()
        .name("ignore-csv-header")
        .displayName("Ignore CSV Header Column Names")
        .description("If the first line of a CSV is a header, and the configured schema does not match the fields named in the header line, this controls how "
            + "the Reader will interpret the fields. If this property is true, then the field names mapped to each column are driven only by the configured schema and "
            + "any fields not in the schema will be ignored. If this property is false, then the field names found in the CSV Header will be used as the names of the "
            + "fields.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(false)
        .build();
    public static final PropertyDescriptor COMMENT_MARKER = new PropertyDescriptor.Builder()
        .name("Comment Marker")
        .description("The character that is used to denote the start of a comment. Any line that begins with this comment will be ignored.")
        .addValidator(new CSVValidators.SingleCharacterValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .build();
    public static final PropertyDescriptor ESCAPE_CHAR = new PropertyDescriptor.Builder()
        .name("Escape Character")
        .description("The character that is used to escape characters that would otherwise have a specific meaning to the CSV Parser. If the property has been specified via Expression Language " +
                "but the expression gets evaluated to an invalid Escape Character at runtime, then it will be skipped and the default Escape Character will be used.")
        .addValidator(new CSVValidators.SingleCharacterValidator())
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue("\\")
        .required(true)
        .build();
    public static final PropertyDescriptor NULL_STRING = new PropertyDescriptor.Builder()
        .name("Null String")
        .description("Specifies a String that, if present as a value in the CSV, should be considered a null field instead of using the literal value.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .build();
    public static final PropertyDescriptor TRIM_FIELDS = new PropertyDescriptor.Builder()
        .name("Trim Fields")
        .description("Whether or not white space should be removed from the beginning and end of fields")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
        .name("csvutils-character-set")
        .displayName("Character Set")
        .description("The Character Encoding that is used to encode/decode the CSV file")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .defaultValue("UTF-8")
        .required(true)
        .build();
    public static final PropertyDescriptor ALLOW_DUPLICATE_HEADER_NAMES = new PropertyDescriptor.Builder()
        .name("csvutils-allow-duplicate-header-names")
        .displayName("Allow Duplicate Header Names")
        .description("Whether duplicate header names are allowed. Header names are case-sensitive, for example \"name\" and \"Name\" are treated as separate fields. " +
                "Handling of duplicate header names is CSV Parser specific (where applicable):\n" +
                "* Apache Commons CSV - duplicate headers will result in column data \"shifting\" right with new fields " +
                "created for \"unknown_field_index_X\" where \"X\" is the CSV column index number\n" +
                "* Jackson CSV - duplicate headers will be de-duplicated with the field value being that of the right-most " +
                "duplicate CSV column")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(false)
        .build();

    // CSV Format fields for writers only
    public static final AllowableValue QUOTE_ALL = new AllowableValue("ALL", "Quote All Values", "All values will be quoted using the configured quote character.");
    public static final AllowableValue QUOTE_MINIMAL = new AllowableValue("MINIMAL", "Quote Minimal",
        "Values will be quoted only if they are contain special characters such as newline characters or field separators.");
    public static final AllowableValue QUOTE_NON_NUMERIC = new AllowableValue("NON_NUMERIC", "Quote Non-Numeric Values", "Values will be quoted unless the value is a number.");
    public static final AllowableValue QUOTE_NONE = new AllowableValue("NONE", "Do Not Quote Values",
        "Values will not be quoted. Instead, all special characters will be escaped using the configured escape character.");

    public static final PropertyDescriptor QUOTE_MODE = new PropertyDescriptor.Builder()
        .name("Quote Mode")
        .description("Specifies how fields should be quoted when they are written")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues(QUOTE_ALL, QUOTE_MINIMAL, QUOTE_NON_NUMERIC, QUOTE_NONE)
        .defaultValue(QUOTE_MINIMAL.getValue())
        .required(true)
        .build();
    public static final PropertyDescriptor TRAILING_DELIMITER = new PropertyDescriptor.Builder()
        .name("Include Trailing Delimiter")
        .description("If true, a trailing delimiter will be added to each CSV Record that is written. If false, the trailing delimiter will be omitted.")
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();
    public static final PropertyDescriptor RECORD_SEPARATOR = new PropertyDescriptor.Builder()
        .name("Record Separator")
        .description("Specifies the characters to use in order to separate CSV Records")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .defaultValue("\\n")
        .required(true)
        .build();
    public static final PropertyDescriptor INCLUDE_HEADER_LINE = new PropertyDescriptor.Builder()
        .name("Include Header Line")
        .description("Specifies whether or not the CSV column names should be written out as the first line.")
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();

    private CSVUtils() {
        // intentionally blank, prevents instantiation
    }

    public static boolean isDynamicCSVFormat(final PropertyContext context) {
        final String formatName = context.getProperty(CSV_FORMAT).getValue();
        return formatName.equalsIgnoreCase(CUSTOM.getValue())
                && (context.getProperty(VALUE_SEPARATOR).isExpressionLanguagePresent()
                || context.getProperty(QUOTE_CHAR).isExpressionLanguagePresent()
                || context.getProperty(ESCAPE_CHAR).isExpressionLanguagePresent()
                || context.getProperty(COMMENT_MARKER).isExpressionLanguagePresent());
    }

    public static CSVFormat createCSVFormat(final PropertyContext context, final Map<String, String> variables) {
        final String formatName = context.getProperty(CSV_FORMAT).getValue();
        if (formatName.equalsIgnoreCase(CUSTOM.getValue())) {
            return buildCustomFormat(context, variables);
        }
        if (formatName.equalsIgnoreCase(RFC_4180.getValue())) {
            return CSVFormat.RFC4180;
        } else if (formatName.equalsIgnoreCase(EXCEL.getValue())) {
            return CSVFormat.EXCEL;
        } else if (formatName.equalsIgnoreCase(TDF.getValue())) {
            return CSVFormat.TDF;
        } else if (formatName.equalsIgnoreCase(MYSQL.getValue())) {
            return CSVFormat.MYSQL;
        } else if (formatName.equalsIgnoreCase(INFORMIX_UNLOAD.getValue())) {
            return CSVFormat.INFORMIX_UNLOAD;
        } else if (formatName.equalsIgnoreCase(INFORMIX_UNLOAD_CSV.getValue())) {
            return CSVFormat.INFORMIX_UNLOAD_CSV;
        } else {
            return CSVFormat.DEFAULT;
        }
    }

    private static Character getValueSeparatorCharUnescapedJava(final PropertyContext context, final Map<String, String> variables) {
        String value = context.getProperty(VALUE_SEPARATOR).evaluateAttributeExpressions(variables).getValue();

        if (value != null) {
            String unescaped = unescape(value);
            if (unescaped.length() == 1) {
                return unescaped.charAt(0);
            }
        }

        LOG.warn("'{}' property evaluated to an invalid value: \"{}\". It must be a single character. The property value will be ignored.", VALUE_SEPARATOR.getName(), value);

        return VALUE_SEPARATOR.getDefaultValue().charAt(0);
    }

    private static Character getCharUnescaped(final PropertyContext context, final PropertyDescriptor property, final Map<String, String> variables) {
        String value = context.getProperty(property).evaluateAttributeExpressions(variables).getValue();

        if (value != null) {
            String unescaped = unescape(value);
            if (unescaped.length() == 1) {
                return unescaped.charAt(0);
            }
        }

        LOG.warn("'{}' property evaluated to an invalid value: \"{}\". It must be a single character. The property value will be ignored.", property.getName(), value);

        if (property.getDefaultValue() != null) {
            return property.getDefaultValue().charAt(0);
        } else {
            return null;
        }
    }

    private static CSVFormat buildCustomFormat(final PropertyContext context, final Map<String, String> variables) {
        final Character valueSeparator = getValueSeparatorCharUnescapedJava(context, variables);
        CSVFormat format = CSVFormat.newFormat(valueSeparator)
            .withAllowMissingColumnNames()
            .withIgnoreEmptyLines();

        final PropertyValue firstLineIsHeaderPropertyValue = context.getProperty(FIRST_LINE_IS_HEADER);
        if (firstLineIsHeaderPropertyValue.getValue() != null && firstLineIsHeaderPropertyValue.asBoolean()) {
            format = format.withFirstRecordAsHeader();
        }

        final Character quoteChar = getCharUnescaped(context, QUOTE_CHAR, variables);
        format = format.withQuote(quoteChar);

        final Character escapeChar = getCharUnescaped(context, ESCAPE_CHAR, variables);
        format = format.withEscape(escapeChar);

        format = format.withTrim(context.getProperty(TRIM_FIELDS).asBoolean());

        if (context.getProperty(COMMENT_MARKER).isSet()) {
            final Character commentMarker = getCharUnescaped(context, COMMENT_MARKER, variables);
            if (commentMarker != null) {
                format = format.withCommentMarker(commentMarker);
            }
        }
        if (context.getProperty(NULL_STRING).isSet()) {
            format = format.withNullString(unescape(context.getProperty(NULL_STRING).getValue()));
        }

        final PropertyValue quoteValue = context.getProperty(QUOTE_MODE);
        if (quoteValue != null && quoteValue.isSet()) {
            final QuoteMode quoteMode = QuoteMode.valueOf(quoteValue.getValue());
            format = format.withQuoteMode(quoteMode);
        }

        final PropertyValue trailingDelimiterValue = context.getProperty(TRAILING_DELIMITER);
        if (trailingDelimiterValue != null && trailingDelimiterValue.isSet()) {
            final boolean trailingDelimiter = trailingDelimiterValue.asBoolean();
            format = format.withTrailingDelimiter(trailingDelimiter);
        }

        final PropertyValue recordSeparator = context.getProperty(RECORD_SEPARATOR);
        if (recordSeparator != null && recordSeparator.isSet()) {
            final String separator = unescape(recordSeparator.getValue());
            format = format.withRecordSeparator(separator);
        }

        final PropertyValue allowDuplicateHeaderNames = context.getProperty(ALLOW_DUPLICATE_HEADER_NAMES);
        if (allowDuplicateHeaderNames != null && allowDuplicateHeaderNames.isSet()) {
            format = format.withAllowDuplicateHeaderNames(allowDuplicateHeaderNames.asBoolean());
        }

        return format;
    }

    public static String unescape(String input) {
        if (input != null && input.length() > 1) {
            input = StringEscapeUtils.unescapeJava(input);
        }
        return input;
    }
}
