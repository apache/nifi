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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.util.MockConfigurationContext;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CSVUtilsTest {

    @Test
    public void testIsDynamicCSVFormatWithStaticProperties() {
        PropertyContext context = createContext("|", "'", "^", "~", "true");

        boolean isDynamicCSVFormat = CSVUtils.isDynamicCSVFormat(context);

        assertFalse(isDynamicCSVFormat);
    }

    @Test
    public void testIsDynamicCSVFormatWithDynamicValueSeparator() {
        PropertyContext context = createContext("${csv.delimiter}", "'", "^", "~", "true");

        boolean isDynamicCSVFormat = CSVUtils.isDynamicCSVFormat(context);

        assertTrue(isDynamicCSVFormat);
    }

    @Test
    public void testIsDynamicCSVFormatWithDynamicQuoteCharacter() {
        PropertyContext context = createContext("|", "${csv.quote}", "^", "~", "true");

        boolean isDynamicCSVFormat = CSVUtils.isDynamicCSVFormat(context);

        assertTrue(isDynamicCSVFormat);
    }

    @Test
    public void testIsDynamicCSVFormatWithDynamicEscapeCharacter() {
        PropertyContext context = createContext("|", "'", "${csv.escape}", "~", "true");

        boolean isDynamicCSVFormat = CSVUtils.isDynamicCSVFormat(context);

        assertTrue(isDynamicCSVFormat);
    }

    @Test
    public void testIsDynamicCSVFormatWithDynamicCommentMarker() {
        PropertyContext context = createContext("|", "'", "^", "${csv.comment}", "true");

        boolean isDynamicCSVFormat = CSVUtils.isDynamicCSVFormat(context);

        assertTrue(isDynamicCSVFormat);
    }

    @Test
    public void testCustomFormat() {
        PropertyContext context = createContext("|", "'", "^", "~", "true");

        CSVFormat csvFormat = CSVUtils.createCSVFormat(context, Collections.emptyMap());

        assertEquals('|', csvFormat.getDelimiter());
        assertEquals('\'', (char) csvFormat.getQuoteCharacter());
        assertEquals('^', (char) csvFormat.getEscapeCharacter());
        assertEquals('~', (char) csvFormat.getCommentMarker());
        assertTrue(csvFormat.getAllowDuplicateHeaderNames());
    }

    @Test
    public void testCustomFormatWithEL() {
        PropertyContext context = createContext("${csv.delimiter}", "${csv.quote}", "${csv.escape}", "${csv.comment}", "false");

        Map<String, String> attributes = new HashMap<>();
        attributes.put("csv.delimiter", "|");
        attributes.put("csv.quote", "'");
        attributes.put("csv.escape", "^");
        attributes.put("csv.comment", "~");

        CSVFormat csvFormat = CSVUtils.createCSVFormat(context, attributes);

        assertEquals('|', csvFormat.getDelimiter());
        assertEquals('\'', (char) csvFormat.getQuoteCharacter());
        assertEquals('^', (char) csvFormat.getEscapeCharacter());
        assertEquals('~', (char) csvFormat.getCommentMarker());
        assertFalse(csvFormat.getAllowDuplicateHeaderNames());
    }

    @Test
    public void testCustomFormatWithELEmptyValues() {
        PropertyContext context = createContext("${csv.delimiter}", "${csv.quote}", "${csv.escape}", "${csv.comment}", "true");

        CSVFormat csvFormat = CSVUtils.createCSVFormat(context, Collections.emptyMap());

        assertEquals(',', csvFormat.getDelimiter());
        assertEquals('"', (char) csvFormat.getQuoteCharacter());
        assertEquals('\\', (char) csvFormat.getEscapeCharacter());
        assertNull(csvFormat.getCommentMarker());
    }

    @Test
    public void testCustomFormatWithELInvalidValues() {
        PropertyContext context = createContext("${csv.delimiter}", "${csv.quote}", "${csv.escape}", "${csv.comment}", "true");

        Map<String, String> attributes = new HashMap<>();
        attributes.put("csv.delimiter", "invalid");
        attributes.put("csv.quote", "invalid");
        attributes.put("csv.escape", "invalid");
        attributes.put("csv.comment", "invalid");

        CSVFormat csvFormat = CSVUtils.createCSVFormat(context, attributes);

        assertEquals(',', csvFormat.getDelimiter());
        assertEquals('"', (char) csvFormat.getQuoteCharacter());
        assertEquals('\\', (char) csvFormat.getEscapeCharacter());
        assertNull(csvFormat.getCommentMarker());
    }

    private PropertyContext createContext(String valueSeparator, String quoteChar, String escapeChar, String commentMarker, String allowDuplicateHeaderNames) {
        Map<PropertyDescriptor, String> properties = new HashMap<>();

        properties.put(CSVUtils.VALUE_SEPARATOR, valueSeparator);
        properties.put(CSVUtils.QUOTE_CHAR, quoteChar);
        properties.put(CSVUtils.ESCAPE_CHAR, escapeChar);
        properties.put(CSVUtils.COMMENT_MARKER, commentMarker);
        properties.put(CSVUtils.ALLOW_DUPLICATE_HEADER_NAMES, allowDuplicateHeaderNames);

        return new MockConfigurationContext(properties, null);
    }
}
