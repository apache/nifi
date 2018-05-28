/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.influxdb.serialization;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.influxdb.impl.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Parse for <a href="https://docs.influxdata.com/influxdb/latest/write_protocols/line_protocol_tutorial/">
 * InfluxDB Line protocol</a>.
 */
public final class InfluxLineProtocolParser {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxLineProtocolParser.class);

    // Internal
    private String lineProtocol;
    private String[] tokens;

    // Public
    private String measurement;
    private Long timestamp;
    private Map<String, String> tags;
    private Map<String, Object> fields;

    /**
     * Create new instance of parser.
     *
     * @param lineProtocol line protocol to pare
     * @return new instance of parser
     * @throws NotParsableInlineProtocolData when data are not parsable
     */
    @NonNull
    public static InfluxLineProtocolParser parse(@Nullable final String lineProtocol)
            throws NotParsableInlineProtocolData {

        if (lineProtocol == null || lineProtocol.isEmpty()) {
            throw new NotParsableInlineProtocolData(lineProtocol, "The Line Protocol does not contains data.");
        }

        return new InfluxLineProtocolParser(lineProtocol.trim())
                .tokenize()
                .syntacticParse();
    }

    private InfluxLineProtocolParser(@NonNull final String lineProtocol) {

        Objects.requireNonNull(lineProtocol, "Line Protocol for parsing is required");

        this.lineProtocol = lineProtocol;
    }

    /**
     * The name of the measurement that you want to write your data to.
     */
    @NonNull
    public String getMeasurement() {
        return measurement;
    }

    /**
     * The tag(s) that you want to include with your data point.
     */
    @NonNull
    public Map<String, String> getTags() {
        return tags;
    }

    /**
     * The field(s) for your data point.
     */
    @NonNull
    public Map<String, Object> getFields() {
        return fields;
    }

    /**
     * The timestamp for your data point in nanosecond-precision Unix time.
     */
    @Nullable
    public Long getTimestamp() {
        return timestamp;
    }

    @NonNull
    private InfluxLineProtocolParser tokenize() throws NotParsableInlineProtocolData {

        // "weather,location=us-midwest,season=summer temperature=82,humidity=54 1465839830100400200" tokenize to:
        //
        // - weather
        // - location=us-midwest,season=summer
        // - temperature=82,humidity=54
        // - 1465839830100400200
        //
        List<String> tokens = tokenizeToList(lineProtocol, new char[]{',', ' '});

        // min length of tokens is 2 max 4
        if (tokens.size() < 2 || tokens.size() > 4) {

            String message = String
                    .format("Unexpected count of tokens: %s (min 2, max 4). Tokens: %s", tokens.size(), tokens);

            throw new NotParsableInlineProtocolData(lineProtocol, message);
        }

        this.tokens = tokens.stream().toArray(String[]::new);

        return this;
    }

    @NonNull
    private InfluxLineProtocolParser syntacticParse() throws NotParsableInlineProtocolData {

        //
        // Data: "weather,location=us-midwest,season=summer temperature=82,humidity=54 1465839830100400200"
        //

        // Measurement
        //
        // For measurements always use a backslash character \ to escape commas ',' and spaces ' '
        //
        // weather
        //
        measurement = tokens[0].replaceAll("\\\\,", ",").replaceAll("\\\\ ", " ");
        if (measurement.isEmpty()) {

            throw new NotParsableInlineProtocolData(lineProtocol,
                    "The Line Protocol does not contains the measurement.");
        }

        // Timestamp
        //
        // 1465839830100400200
        //
        if (tokens.length > 2) {

            String token;
            try {

                token = tokens[tokens.length - 1].trim();

                //
                // Parse as Long
                //
                try {

                    timestamp = Long.parseLong(token);

                } catch (NumberFormatException e) {

                    //
                    // Parse as RFC3339
                    //
                    if (!token.isEmpty()) {
                        timestamp = TimeUtil.fromInfluxDBTimeFormat(token);
                    }
                }
            } catch (Exception e) {

                // measurement is required
                if (tokens.length == 4) {
                    throw new NotParsableInlineProtocolData(lineProtocol, e);
                }
            }
        }

        // Tags & Fields
        //
        // [0] - location=us-midwest,season=summer
        // [1] - temperature=82,humidity=54
        //
        String[] tagsAndFields = ArrayUtils
                .subarray(tokens, 1, timestamp != null ? tokens.length - 1 : tokens.length);

        // Tags
        //
        // location=us-midwest,season=summer parsed to:
        //
        // - location;us-midwest
        // - season;summer
        //
        tags = syntacticParseMap(tagsAndFields.length == 2 ? tagsAndFields[0] : null, ValueTransformer.TO_STRING);

        // Fields
        //
        // temperature=82,humidity=54 parsed to:
        //
        // - temperature;us-82
        // - humidity;54
        //
        fields = syntacticParseMap(tagsAndFields[tagsAndFields.length - 1], ValueTransformer.TO_OBJECT);

        return this;
    }

    /**
     * Log the parsing result.
     */
    @NonNull
    protected InfluxLineProtocolParser report() {

        LOG.debug("The Line Protocol: '{}' was parsed to '{}' tokens.", lineProtocol, tokens);

        return this;
    }

    @NonNull
    private <V> Map<String, V> syntacticParseMap(@Nullable final String protocolBlock,
                                                 @NonNull final ValueTransformer transformer)

            throws NotParsableInlineProtocolData {

        Objects.requireNonNull(transformer, "ValueTransformer is required");

        Map<String, V> results = new HashMap<>();

        // location=us-midwest,season=summer parsed to:
        //
        // - location=us-midwest
        // - season=summer
        //
        List<String> tokens = tokenizeToList(protocolBlock, new char[]{','});

        for (String keyValue : tokens) {

            // location=us-midwest parsed to:
            //
            // - location
            // - us-midwest
            //
            List<String> strings = tokenizeToList(keyValue, new char[]{'='});

            // key and value are required
            if (strings.size() != 2) {
                throw new NotParsableInlineProtocolData(lineProtocol, "The Line Protocol does not contains fields.");
            }

            //
            // Parse to Java Object: String, Float, Long, ...
            //
            V value;
            try {
                //noinspection unchecked
                value = (V) transformer.transformValue(strings.get(1));
            } catch (Exception e) {
                throw new NotParsableInlineProtocolData(lineProtocol, e);
            }

            //
            // Put result
            //
            results.put(removeEscaping(strings.get(0)), value);
        }

        return results;
    }

    private enum EscapeMode {

        NONE,

        // Escaping by "\"
        SIMPLE,

        // Escaping by "Some text"
        TEXT
    }

    @NonNull
    private List<String> tokenizeToList(@Nullable final String protocolBlock, final char[] delimiters)
            throws NotParsableInlineProtocolData {

        List<String> tokens = new ArrayList<>();
        if (protocolBlock == null) {
            return tokens;
        }

        String token = "";
        EscapeMode escapeMode = EscapeMode.NONE;
        char[] blockDelimiters = delimiters;

        for (int i = 0; i < protocolBlock.length(); i++) {

            char charAt = protocolBlock.charAt(i);

            // Is it the finished block of LineProtocol ?
            if (ArrayUtils.contains(blockDelimiters, charAt) && !escapeMode.equals(EscapeMode.TEXT)) {

                // finish escaping
                if (EscapeMode.SIMPLE.equals(escapeMode)) {

                    escapeMode = EscapeMode.NONE;

                } else {

                    if (!token.isEmpty()) {
                        tokens.add(token);
                    } else {
                        throw new NotParsableInlineProtocolData(lineProtocol, "The Line Protocol block is empty.");
                    }

                    //
                    // If we have defined more delimiters than after first 'hit' use the seconds
                    //
                    // => first we parser by ',' (measurement,tags) than after ' ' (tags fields timestamp)
                    //
                    blockDelimiters = new char[]{delimiters[delimiters.length - 1]};
                    token = "";

                    continue;
                }
            }

            // SIMPLE escaping START
            //
            if (charAt == '\\' && !EscapeMode.TEXT.equals(escapeMode)) {

                escapeMode = EscapeMode.SIMPLE;

            // SIMPLE escaping END
            //
            } else if (EscapeMode.SIMPLE.equals(escapeMode)) {

                escapeMode = EscapeMode.NONE;

            // TEXT escaping START
            //
            } else if (charAt == '\"' && (i > 0 && protocolBlock.charAt(i - 1) == '=')) {
                escapeMode = EscapeMode.TEXT;

            // TEXT escaping END
            //
            } else if (EscapeMode.TEXT.equals(escapeMode) && charAt == '\"') {

                if (!token.endsWith("\\") || token.endsWith("\\\\")) {
                    escapeMode = EscapeMode.NONE;
                }
            }

            token += charAt;
        }

        if (!token.isEmpty()) {
            tokens.add(token);
        }

        return tokens;
    }

    private enum ValueTransformer {

        TO_STRING {
            @Override
            Object transformValue(@NonNull final String value) {
                return removeEscaping(value);
            }
        },

        TO_OBJECT {
            @Override
            Object transformValue(@NonNull final String value) {

                if (value == null) {
                    return null;
                }

                if (value.endsWith("i")) {

                    return Long.parseLong(value.substring(0, value.length() - 1));
                }

                Boolean bool = BooleanUtils.toBooleanObject(value);
                if (bool != null) {
                    return bool;
                }

                //
                // String
                //
                // "Some text" => Some text
                //
                if (value.startsWith("\"") && value.endsWith("\"")) {

                    String unquoted = value.substring(1, value.length() - 1);

                    //
                    // For string field values use a backslash character \ to escape double quotes "
                    //
                    return unquoted.replace("\\\"", "\"").replace("\\\\", "\\");
                }

                //
                // Float
                //
                Float number = Float.parseFloat(value);
                if (number.isInfinite()) {
                    throw new IllegalArgumentException(value + " is infinite Float value");
                }
                if (number.isNaN()) {
                    throw new IllegalArgumentException(value + " is Not-a-Number");
                }

                return number;
            }
        };

        abstract Object transformValue(@NonNull final String value);
    }

    /**
     * For tag keys, tag values, and field keys always use a backslash character \ to escape commas ',',
     * equal signs '=' and spaces ' '.
     */
    @NonNull
    private static String removeEscaping(@NonNull final String value) {
        //
        // For tag keys, tag values, and field keys always use a backslash character \ to escape:
        //
        // commas ','
        // equal signs '='
        // spaces ' '
        //
        return value.replaceAll("\\\\=", "=").replaceAll("\\\\,", ",").replaceAll("\\\\ ", " ");
    }
}
