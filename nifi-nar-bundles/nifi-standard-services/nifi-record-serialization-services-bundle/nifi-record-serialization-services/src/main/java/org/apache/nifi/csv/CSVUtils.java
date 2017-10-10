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
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

public class CSVUtils {

    static final AllowableValue CUSTOM = new AllowableValue("custom", "Custom Format",
        "The format of the CSV is configured by using the properties of this Controller Service, such as Value Separator");
    static final AllowableValue RFC_4180 = new AllowableValue("rfc-4180", "RFC 4180", "CSV data follows the RFC 4180 Specification defined at https://tools.ietf.org/html/rfc4180");
    static final AllowableValue EXCEL = new AllowableValue("excel", "Microsoft Excel", "CSV data follows the format used by Microsoft Excel");
    static final AllowableValue TDF = new AllowableValue("tdf", "Tab-Delimited", "CSV data is Tab-Delimited instead of Comma Delimited");
    static final AllowableValue INFORMIX_UNLOAD = new AllowableValue("informix-unload", "Informix Unload", "The format used by Informix when issuing the UNLOAD TO file_name command");
    static final AllowableValue INFORMIX_UNLOAD_CSV = new AllowableValue("informix-unload-csv", "Informix Unload Escape Disabled",
        "The format used by Informix when issuing the UNLOAD TO file_name command with escaping disabled");
    static final AllowableValue MYSQL = new AllowableValue("mysql", "MySQL Format", "CSV data follows the format used by MySQL");

    static final PropertyDescriptor CSV_FORMAT = new PropertyDescriptor.Builder()
        .name("CSV Format")
        .description("Specifies which \"format\" the CSV data is in, or specifies if custom formatting should be used.")
        .expressionLanguageSupported(false)
        .allowableValues(CUSTOM, RFC_4180, EXCEL, TDF, MYSQL, INFORMIX_UNLOAD, INFORMIX_UNLOAD_CSV)
        .defaultValue(CUSTOM.getValue())
        .required(true)
        .build();
    static final PropertyDescriptor VALUE_SEPARATOR = new PropertyDescriptor.Builder()
        .name("Value Separator")
        .description("The character that is used to separate values/fields in a CSV Record")
        .addValidator(CSVValidators.UNESCAPED_SINGLE_CHAR_VALIDATOR)
        .expressionLanguageSupported(false)
        .defaultValue(",")
        .required(true)
        .build();
    static final PropertyDescriptor QUOTE_CHAR = new PropertyDescriptor.Builder()
        .name("Quote Character")
        .description("The character that is used to quote values so that escape characters do not have to be used")
        .addValidator(new CSVValidators.SingleCharacterValidator())
        .expressionLanguageSupported(false)
        .defaultValue("\"")
        .required(true)
        .build();
    static final PropertyDescriptor FIRST_LINE_IS_HEADER = new PropertyDescriptor.Builder()
        .name("Skip Header Line")
        .displayName("Treat First Line as Header")
        .description("Specifies whether or not the first line of CSV should be considered a Header or should be considered a record. If the Schema Access Strategy "
            + "indicates that the columns must be defined in the header, then this property will be ignored, since the header must always be "
            + "present and won't be processed as a Record. Otherwise, if 'true', then the first line of CSV data will not be processed as a record and if 'false',"
            + "then the first line will be interpreted as a record.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();
    static final PropertyDescriptor IGNORE_CSV_HEADER = new PropertyDescriptor.Builder()
        .name("ignore-csv-header")
        .displayName("Ignore CSV Header Column Names")
        .description("If the first line of a CSV is a header, and the configured schema does not match the fields named in the header line, this controls how "
            + "the Reader will interpret the fields. If this property is true, then the field names mapped to each column are driven only by the configured schema and "
            + "any fields not in the schema will be ignored. If this property is false, then the field names found in the CSV Header will be used as the names of the "
            + "fields.")
        .expressionLanguageSupported(false)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(false)
        .build();
    static final PropertyDescriptor COMMENT_MARKER = new PropertyDescriptor.Builder()
        .name("Comment Marker")
        .description("The character that is used to denote the start of a comment. Any line that begins with this comment will be ignored.")
        .addValidator(new CSVValidators.SingleCharacterValidator())
        .expressionLanguageSupported(false)
        .required(false)
        .build();
    static final PropertyDescriptor ESCAPE_CHAR = new PropertyDescriptor.Builder()
        .name("Escape Character")
        .description("The character that is used to escape characters that would otherwise have a specific meaning to the CSV Parser.")
        .addValidator(new CSVValidators.SingleCharacterValidator())
        .expressionLanguageSupported(false)
        .defaultValue("\\")
        .required(true)
        .build();
    static final PropertyDescriptor NULL_STRING = new PropertyDescriptor.Builder()
        .name("Null String")
        .description("Specifies a String that, if present as a value in the CSV, should be considered a null field instead of using the literal value.")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .required(false)
        .build();
    static final PropertyDescriptor TRIM_FIELDS = new PropertyDescriptor.Builder()
        .name("Trim Fields")
        .description("Whether or not white space should be removed from the beginning and end of fields")
        .expressionLanguageSupported(false)
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();

    // CSV Format fields for writers only
    static final AllowableValue QUOTE_ALL = new AllowableValue("ALL", "Quote All Values", "All values will be quoted using the configured quote character.");
    static final AllowableValue QUOTE_MINIMAL = new AllowableValue("MINIMAL", "Quote Minimal",
        "Values will be quoted only if they are contain special characters such as newline characters or field separators.");
    static final AllowableValue QUOTE_NON_NUMERIC = new AllowableValue("NON_NUMERIC", "Quote Non-Numeric Values", "Values will be quoted unless the value is a number.");
    static final AllowableValue QUOTE_NONE = new AllowableValue("NONE", "Do Not Quote Values",
        "Values will not be quoted. Instead, all special characters will be escaped using the configured escape character.");

    static final PropertyDescriptor QUOTE_MODE = new PropertyDescriptor.Builder()
        .name("Quote Mode")
        .description("Specifies how fields should be quoted when they are written")
        .expressionLanguageSupported(false)
        .allowableValues(QUOTE_ALL, QUOTE_MINIMAL, QUOTE_NON_NUMERIC, QUOTE_NONE)
        .defaultValue(QUOTE_MINIMAL.getValue())
        .required(true)
        .build();
    static final PropertyDescriptor TRAILING_DELIMITER = new PropertyDescriptor.Builder()
        .name("Include Trailing Delimiter")
        .description("If true, a trailing delimiter will be added to each CSV Record that is written. If false, the trailing delimiter will be omitted.")
        .expressionLanguageSupported(false)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();
    static final PropertyDescriptor RECORD_SEPARATOR = new PropertyDescriptor.Builder()
        .name("Record Separator")
        .description("Specifies the characters to use in order to separate CSV Records")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .defaultValue("\\n")
        .required(true)
        .build();
    static final PropertyDescriptor INCLUDE_HEADER_LINE = new PropertyDescriptor.Builder()
        .name("Include Header Line")
        .description("Specifies whether or not the CSV column names should be written out as the first line.")
        .allowableValues("true", "false")
        .defaultValue("true")
        .required(true)
        .build();

    static CSVFormat createCSVFormat(final ConfigurationContext context) {
        final String formatName = context.getProperty(CSV_FORMAT).getValue();
        if (formatName.equalsIgnoreCase(CUSTOM.getValue())) {
            return buildCustomFormat(context);
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

    private static char getUnescapedChar(final ConfigurationContext context, final PropertyDescriptor property) {
        return StringEscapeUtils.unescapeJava(context.getProperty(property).getValue()).charAt(0);
    }

    private static char getChar(final ConfigurationContext context, final PropertyDescriptor property) {
        return CSVUtils.unescape(context.getProperty(property).getValue()).charAt(0);
    }

    private static CSVFormat buildCustomFormat(final ConfigurationContext context) {
        final char valueSeparator = getUnescapedChar(context, VALUE_SEPARATOR);
        CSVFormat format = CSVFormat.newFormat(valueSeparator)
            .withAllowMissingColumnNames()
            .withIgnoreEmptyLines();

        final PropertyValue skipHeaderPropertyValue = context.getProperty(FIRST_LINE_IS_HEADER);
        if (skipHeaderPropertyValue.getValue() != null && skipHeaderPropertyValue.asBoolean()) {
            format = format.withFirstRecordAsHeader();
        }

        format = format.withQuote(getChar(context, QUOTE_CHAR));
        format = format.withEscape(getChar(context, ESCAPE_CHAR));
        format = format.withTrim(context.getProperty(TRIM_FIELDS).asBoolean());

        if (context.getProperty(COMMENT_MARKER).isSet()) {
            format = format.withCommentMarker(getChar(context, COMMENT_MARKER));
        }
        if (context.getProperty(NULL_STRING).isSet()) {
            format = format.withNullString(CSVUtils.unescape(context.getProperty(NULL_STRING).getValue()));
        }

        final PropertyValue quoteValue = context.getProperty(QUOTE_MODE);
        if (quoteValue != null) {
            final QuoteMode quoteMode = QuoteMode.valueOf(quoteValue.getValue());
            format = format.withQuoteMode(quoteMode);
        }

        final PropertyValue trailingDelimiterValue = context.getProperty(TRAILING_DELIMITER);
        if (trailingDelimiterValue != null) {
            final boolean trailingDelimiter = trailingDelimiterValue.asBoolean();
            format = format.withTrailingDelimiter(trailingDelimiter);
        }

        final PropertyValue recordSeparator = context.getProperty(RECORD_SEPARATOR);
        if (recordSeparator != null) {
            final String separator = unescape(recordSeparator.getValue());
            format = format.withRecordSeparator(separator);
        }

        return format;
    }


    public static String unescape(final String input) {
        if (input == null) {
            return input;
        }

        return input.replace("\\t", "\t")
            .replace("\\n", "\n")
            .replace("\\r", "\r");
    }
}
