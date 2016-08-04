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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


public class TestParseCEF {
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

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
        mff.assertAttributeEquals("cef.extension.rt", sdf.format(new Date(1423441663000L)));
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


        // Converting a field without timezone will always result on render time being dependent
        // on locale of the machine running this test.
        long eventTime = 1423229263000L;
        int offset = TimeZone.getDefault().getOffset(eventTime);
        sdf.setTimeZone(TimeZone.getDefault());

        String prettyEvent = sdf.format(new Date(eventTime - offset));

        mff.assertAttributeEquals("cef.extension.deviceCustomDate1",prettyEvent);
        mff.assertAttributeEquals("cef.extension.dpt", "1234");
        mff.assertAttributeEquals("cef.extension.agt", "123.123.0.124");
        mff.assertAttributeEquals("cef.extension.dlat", "40.366633");
    }

    @Test
    public void testSuccessfulParseToAttributesWithUTC() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ParseCEF());
        runner.setProperty(ParseCEF.FIELDS_DESTINATION, ParseCEF.DESTINATION_ATTRIBUTES);
        runner.setProperty(ParseCEF.TIME_REPRESENTATION, ParseCEF.UTC);
        runner.enqueue(sample1.getBytes());
        runner.run();

        sdf.setTimeZone(TimeZone.getTimeZone(ParseCEF.UTC));

        runner.assertAllFlowFilesTransferred(ParseCEF.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ParseCEF.REL_SUCCESS).get(0);
        mff.assertAttributeEquals("cef.extension.rt", sdf.format(new Date(1423441663000L)));

        // Converting a field without timezone will always result on render time being dependent
        // on locale of the machine running this test.
        long eventTime = 1423229263000L;
        int offset = TimeZone.getDefault().getOffset(eventTime);
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        String prettyEvent = sdf.format(new Date(eventTime - offset));

        mff.assertAttributeEquals("cef.extension.deviceCustomDate1",prettyEvent);
        mff.assertContentEquals(sample1.getBytes());
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

        Assert.assertEquals("TestVendor", header.get("deviceVendor").asText());
        Assert.assertEquals(sdf.format(new Date(1423441663000L)),
                            extension.get("rt").asText());
        Assert.assertEquals("Test Long", extension.get("cn3Label").asText());
        Assert.assertEquals( 9223372036854775807L, extension.get("cn3").asLong());
        Assert.assertTrue(extension.get("cfp1").floatValue() == 1.234F);
        Assert.assertEquals("Test FP Number", extension.get("cfp1Label").asText());
        Assert.assertEquals("00:00:0c:07:ac:00", extension.get("smac").asText());
        Assert.assertEquals("2001:cdba:0:0:0:0:3257:9652", extension.get("c6a3").asText());
        Assert.assertEquals("Test IPv6", extension.get("c6a3Label").asText());
        Assert.assertEquals("123.123.123.123", extension.get("destinationTranslatedAddress").asText());
        Assert.assertEquals("Test String", extension.get("cs1Label").asText());
        Assert.assertEquals("test test test chocolate", extension.get("cs1").asText());
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

        Assert.assertEquals("TestVendor", header.get("deviceVendor").asText());
        Assert.assertEquals(sdf.format(new Date(1423441663000L)),
                extension.get("rt").asText());
        Assert.assertEquals("Test Long", extension.get("cn3Label").asText());
        Assert.assertEquals( 9223372036854775807L, extension.get("cn3").asLong());
        Assert.assertTrue(extension.get("cfp1").floatValue() == 1.234F);
        Assert.assertEquals("Test FP Number", extension.get("cfp1Label").asText());
        Assert.assertEquals("00:00:0c:07:ac:00", extension.get("smac").asText());
        Assert.assertEquals("2001:cdba:0:0:0:0:3257:9652", extension.get("c6a3").asText());
        Assert.assertEquals("Test IPv6", extension.get("c6a3Label").asText());
        Assert.assertEquals("Test String", extension.get("cs1Label").asText());
        Assert.assertEquals("test test test chocolate", extension.get("cs1").asText());
        Assert.assertEquals("123.123.123.123", extension.get("destinationTranslatedAddress").asText());

        JsonNode inner = new ObjectMapper().readTree(extension.get("cs2").asText());
        Assert.assertEquals("chocolate!", inner.get("test_test_test").asText());
    }


    @Test
    public void testSuccessfulParseToContentUTC() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ParseCEF());
        runner.setProperty(ParseCEF.FIELDS_DESTINATION, ParseCEF.DESTINATION_CONTENT);
        runner.setProperty(ParseCEF.TIME_REPRESENTATION, ParseCEF.UTC);
        runner.enqueue(sample1.getBytes());
        runner.run();

        runner.assertAllFlowFilesTransferred(ParseCEF.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ParseCEF.REL_SUCCESS).get(0);

        byte [] rawJson = mff.toByteArray();

        JsonNode results = new ObjectMapper().readTree(rawJson);

        JsonNode extension = results.get("extension");

        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        Assert.assertEquals(sdf.format(new Date(1423441663000L)),
                extension.get("rt").asText());

        // Converting a field without timezone will always result on render time being dependent
        // on locale of the machine running this test.
        long eventTime = 1423229263000L;
        int offset = TimeZone.getDefault().getOffset(eventTime);

        // Set TZ to UTC
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

        String prettyEvent = sdf.format(new Date(eventTime - offset));
        Assert.assertEquals(prettyEvent, extension.get("deviceCustomDate1").asText());
    }


}

