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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestParseCEF {
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private static final ZonedDateTime DATE_TIME = Instant.ofEpochMilli(1423441663000L).atZone(ZoneId.systemDefault());

    private final static String sample1 = "CEF:0|TestVendor|TestProduct|TestVersion|TestEventClassID|TestName|Low|" +
            // TimeStamp, String and Long
            "rt=Feb 09 2015 00:27:43 UTC cn3Label=Test Long cn3=9223372036854775807 " +
            // FloatPoint and MacAddress
            "cfp1=1.234 cfp1Label=Test FP Number smac=00:00:0c:07:ac:00 " +
            // IPv6 and String
            "c6a3=2001:cdba::3257:9652 c6a3Label=Test IPv6 cs1Label=Test String cs1=test test test chocolate " +
            // IPv4
            "destinationTranslatedAddress=123.123.123.123 " +
            // Date without TZ
            "deviceCustomDate1=Feb 06 2015 13:27:43 " +
            // Integer  and IP Address (from v4)
            "dpt=1234 agt=123.123.0.124 dlat=40.366633";

    private final static String sample2 = "CEF:0|TestVendor|TestProduct|TestVersion|TestEventClassID|TestName|Low|" +
            // TimeStamp, String and Long
            "rt=Feb 09 2015 00:27:43 UTC cn3Label=Test Long cn3=9223372036854775807 " +
            // FloatPoint and MacAddress
            "cfp1=1.234 cfp1Label=Test FP Number smac=00:00:0c:07:ac:00 " +
            // IPv6 and String
            "c6a3=2001:cdba::3257:9652 c6a3Label=Test IPv6 cs1Label=Test String cs1=test test test chocolate " +
            // IPv4
            "destinationTranslatedAddress=123.123.123.123 " +
            // Date without TZ
            "deviceCustomDate1=Feb 06 2015 13:27:43 " +
            // Integer  and IP Address (from v4)
            "dpt=1234 agt=123.123.0.124 dlat=40.366633 " +
            // A JSON object inside one of CEF's custom Strings
            "cs2Label=JSON payload " +
            "cs2={\"test_test_test\": \"chocolate!\", \"what?!?\": \"Simple! test test test chocolate!\"}";


    @Test
    public void testInvalidMessage() {
        final TestRunner runner = TestRunners.newTestRunner(new ParseCEF());
        runner.enqueue("test test test chocolate\n".getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ParseCEF.REL_FAILURE, 1);
    }

    @Test
    public void testSuccessfulParseToAttributes() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ParseCEF());
        runner.setProperty(ParseCEF.FIELDS_DESTINATION, ParseCEF.DESTINATION_ATTRIBUTES);
        runner.enqueue(sample1.getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ParseCEF.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ParseCEF.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("cef.extension.rt", FORMATTER.format(DATE_TIME));
        mff.assertAttributeEquals("cef.extension.cn3Label", "Test Long");
        mff.assertAttributeEquals("cef.extension.cn3", "9223372036854775807");
        mff.assertAttributeEquals("cef.extension.cfp1", "1.234");
        mff.assertAttributeEquals("cef.extension.cfp1Label", "Test FP Number");
        mff.assertAttributeEquals("cef.extension.smac", "00:00:0c:07:ac:00");
        mff.assertAttributeEquals("cef.extension.c6a3", "2001:cdba:0:0:0:0:3257:9652");
        mff.assertAttributeEquals("cef.extension.c6a3Label", "Test IPv6");
        mff.assertAttributeEquals("cef.extension.cs1Label", "Test String");
        mff.assertAttributeEquals("cef.extension.cs1", "test test test chocolate");
        mff.assertAttributeEquals("cef.extension.destinationTranslatedAddress", "123.123.123.123");
        mff.assertContentEquals(sample1.getBytes());

        mff.assertAttributeEquals("cef.extension.dpt", "1234");
        mff.assertAttributeEquals("cef.extension.agt", "123.123.0.124");
        mff.assertAttributeEquals("cef.extension.dlat", "40.366633");
    }

    @Test
    public void testSuccessfulParseToContent() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ParseCEF());
        runner.setProperty(ParseCEF.FIELDS_DESTINATION, ParseCEF.DESTINATION_CONTENT);
        runner.enqueue(sample1.getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ParseCEF.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ParseCEF.REL_SUCCESS).get(0);

        byte [] rawJson = mff.toByteArray();

        JsonNode results = new ObjectMapper().readTree(rawJson);

        JsonNode header = results.get("header");
        JsonNode extension = results.get("extension");

        assertEquals("TestVendor", header.get("deviceVendor").asText());
        assertEquals(FORMATTER.format(DATE_TIME),
                            extension.get("rt").asText());
        assertEquals("Test Long", extension.get("cn3Label").asText());
        assertEquals( 9223372036854775807L, extension.get("cn3").asLong());
        assertEquals(extension.get("cfp1").floatValue(), 1.234F);
        assertEquals("Test FP Number", extension.get("cfp1Label").asText());
        assertEquals("00:00:0c:07:ac:00", extension.get("smac").asText());
        assertEquals("2001:cdba:0:0:0:0:3257:9652", extension.get("c6a3").asText());
        assertEquals("Test IPv6", extension.get("c6a3Label").asText());
        assertEquals("123.123.123.123", extension.get("destinationTranslatedAddress").asText());
        assertEquals("Test String", extension.get("cs1Label").asText());
        assertEquals("test test test chocolate", extension.get("cs1").asText());
    }

    @Test
    public void testSuccessfulParseToContentWhenCEFContainsJSON() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ParseCEF());
        runner.setProperty(ParseCEF.FIELDS_DESTINATION, ParseCEF.DESTINATION_CONTENT);
        runner.enqueue(sample2.getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ParseCEF.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ParseCEF.REL_SUCCESS).get(0);

        byte [] rawJson = mff.toByteArray();

        JsonNode results = new ObjectMapper().readTree(rawJson);

        JsonNode header = results.get("header");
        JsonNode extension = results.get("extension");

        assertEquals("TestVendor", header.get("deviceVendor").asText());
        assertEquals(FORMATTER.format(DATE_TIME),
                extension.get("rt").asText());
        assertEquals("Test Long", extension.get("cn3Label").asText());
        assertEquals( 9223372036854775807L, extension.get("cn3").asLong());
        assertEquals(extension.get("cfp1").floatValue(), 1.234F);
        assertEquals("Test FP Number", extension.get("cfp1Label").asText());
        assertEquals("00:00:0c:07:ac:00", extension.get("smac").asText());
        assertEquals("2001:cdba:0:0:0:0:3257:9652", extension.get("c6a3").asText());
        assertEquals("Test IPv6", extension.get("c6a3Label").asText());
        assertEquals("Test String", extension.get("cs1Label").asText());
        assertEquals("test test test chocolate", extension.get("cs1").asText());
        assertEquals("123.123.123.123", extension.get("destinationTranslatedAddress").asText());

        JsonNode inner = new ObjectMapper().readTree(extension.get("cs2").asText());
        assertEquals("chocolate!", inner.get("test_test_test").asText());
    }

    @Test
    public void testCustomValidator() {
        final TestRunner runner = TestRunners.newTestRunner(new ParseCEF());
        runner.setProperty(ParseCEF.FIELDS_DESTINATION, ParseCEF.DESTINATION_CONTENT);
        runner.setProperty(ParseCEF.TIME_REPRESENTATION, ParseCEF.UTC);


        runner.setProperty(ParseCEF.DATETIME_REPRESENTATION, "SPANGLISH");
        runner.assertNotValid();

        runner.setProperty(ParseCEF.DATETIME_REPRESENTATION, "en-US");
        runner.assertValid();

        Locale[] availableLocales = Locale.getAvailableLocales();
        for (Locale listedLocale : availableLocales ) {
            if (!listedLocale.toString().isEmpty()) {
                String input = listedLocale.toLanguageTag();
                runner.setProperty(ParseCEF.DATETIME_REPRESENTATION, input );
                runner.assertValid();
            }
        }
    }

    @Test
    public void testIncludeCustomExtensions() throws Exception {
        String sample3 = "<159>Aug 09 08:56:28 8.8.8.8 CEF:0|x|Security|x.x.0|20|Transaction blocked|7| "
            + "act=blocked app=https dvc=8.8.8.8 dst=8.8.8.8 dhost=www.flynas.com dpt=443 src=8.8.8.8 "
            + "spt=53475 suser=x UserPath=LDAP://8.8.8.8 OU\\\\=1 - x x x x,OU\\\\=x x,DC\\\\=x,DC\\\\=com/x "
            + "destinationTranslatedPort=36436 rt=1628488588000 in=65412 out=546 requestMethod=GET  "
            + "category=20 http_response=200 http_proxy_status_code=302 duration=13 "
            + "requestClientApplication=Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0;) like Gecko reason=-  "
            + "cs1Label=Policy cs1=x x**x cs2Label=DynCat cs2=0 cs3Label=ContentType cs3=font/otf "
            + "cn1Label=DispositionCode cn1=1047 cn2Label=ScanDuration cn2=13 "
            + "request=https://www.flynas.com/css/fonts/GothamRounded-Book.otf URLRefer=https://www.flynas.com/en";

        final TestRunner runner = TestRunners.newTestRunner(new ParseCEF());
        runner.setProperty(ParseCEF.FIELDS_DESTINATION, ParseCEF.DESTINATION_CONTENT);
        runner.setProperty(ParseCEF.TIME_REPRESENTATION, ParseCEF.UTC);
        runner.setProperty(ParseCEF.INCLUDE_CUSTOM_EXTENSIONS, "true");
        runner.enqueue(sample3.getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ParseCEF.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ParseCEF.REL_SUCCESS).get(0);

        byte [] rawJson = mff.toByteArray();

        JsonNode results = new ObjectMapper().readTree(rawJson);

        JsonNode extension = results.get("extension");
        assertEquals(200, extension.get("http_response").asInt());
    }

    @Test
    public void testAcceptEmptyExtensions() throws Exception {
        String sample3 = "CEF:0|TestVendor|TestProduct|TestVersion|TestEventClassID|TestName|Low|" +
                "rt=Feb 09 2015 00:27:43 UTC cn3Label= cn3= " +
                "cfp1=1.234 cfp1Label=Test FP Number smac=00:00:0c:07:ac:00 " +
                "c6a3=2001:cdba::3257:9652 c6a3Label=Test IPv6 cs1Label=Test String cs1=test test test chocolate " +
                "destinationTranslatedAddress=123.123.123.123 " +
                "deviceCustomDate1=Feb 06 2015 13:27:43 " +
                "dpt= agt= dlat=";

        final TestRunner runner = TestRunners.newTestRunner(new ParseCEF());
        runner.setProperty(ParseCEF.FIELDS_DESTINATION, ParseCEF.DESTINATION_CONTENT);
        runner.setProperty(ParseCEF.TIME_REPRESENTATION, ParseCEF.UTC);
        runner.setProperty(ParseCEF.INCLUDE_CUSTOM_EXTENSIONS, "true");
        runner.setProperty(ParseCEF.ACCEPT_EMPTY_EXTENSIONS, "true");
        runner.enqueue(sample3.getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ParseCEF.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ParseCEF.REL_SUCCESS).get(0);

        byte [] rawJson = mff.toByteArray();

        JsonNode results = new ObjectMapper().readTree(rawJson);

        JsonNode extensions = results.get("extension");
        assertTrue(extensions.has("cn3"));
        assertTrue(extensions.get("cn3").isNull());

        assertTrue(extensions.has("cn3Label"));
        assertTrue(extensions.get("cn3Label").asText().isEmpty());
    }
}

