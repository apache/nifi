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
package org.apache.nifi.processors.hl7;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import static org.junit.Assert.assertEquals;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class TestExtractHL7Attributes {

    private void runTests(final String message, final Map<String, String> expectedAttributes, final boolean useSegmentNames, final boolean parseSegmentFields) {
        final TestRunner runner = TestRunners.newTestRunner(ExtractHL7Attributes.class);
        runner.setProperty(ExtractHL7Attributes.USE_SEGMENT_NAMES, String.valueOf(useSegmentNames));
        runner.setProperty(ExtractHL7Attributes.PARSE_SEGMENT_FIELDS, String.valueOf(parseSegmentFields));
        runner.enqueue(message.getBytes(StandardCharsets.UTF_8));

        runner.run();
        runner.assertAllFlowFilesTransferred(ExtractHL7Attributes.REL_SUCCESS, 1);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(ExtractHL7Attributes.REL_SUCCESS).get(0);

        final SortedMap<String, String> actualAttributes = new TreeMap<>(flowFile.getAttributes());
        final SortedMap<String, Integer> actualSegments = new TreeMap<>();
        final SortedMap<String, Integer> expectedSegments = new TreeMap<>();

        // Get map of actual segment counts
        for (final Map.Entry<String, String> entry : actualAttributes.entrySet()) {
            final String key = entry.getKey();
            if (!(key.equals("filename") || key.equals("path") || key.equals("uuid"))) {
                final String segment = key.replaceAll("^([^\\.]+)\\.\\d+", "$1");
                actualSegments.put(segment, actualSegments.getOrDefault(segment, 0) + 1);
            }
        }

        // Get map of expected segment counts
        for (final Map.Entry<String, String> entry : expectedAttributes.entrySet()) {
            final String key = entry.getKey();
            final String segment = key.replaceAll("^([^\\.]+)\\.\\d+", "$1");
            expectedSegments.put(segment, expectedSegments.getOrDefault(segment, 0) + 1);
        }

        // First, check whether the actual and expected segment counts are the same
        assertEquals("Segment counts do not match", expectedSegments, actualSegments);

        // Check whether the actual and expected segment field values are the same
        for (final Map.Entry<String, String> entry : expectedAttributes.entrySet()) {
            final String key = entry.getKey();
            final String expected = entry.getValue();
            final String actual = actualAttributes.get(key);
            assertEquals(key + " segment value does not match", expected, actual);
        }
    }

    private void runTests(final String message, final Map<String, String> expectedAttributes) {
        runTests(message, expectedAttributes, false, false);
    }

    @BeforeClass
    public static void setup() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi", "DEBUG");
    }

    @Test
    public void testExtract() {
        final String message = "MSH|^~\\&|XXXXXXXX||HealthProvider||||ORU^R01|Q1111111111111111111|P|2.3|\r\n" +
            "PID|||12345^^^XYZ^MR||SMITH^JOHN||19700100|M||||||||||111111111111|123456789|\r\n" +
            "PD1||||1234567890^LAST^FIRST^M^^^^^NPI|\r\n" +
            "ORC|NW|987654321^EPC|123456789^EPC||||||20161003000000|||SMITH\r\n" +
            "OBR|1|341856649^HNAM_ORDERID|000000000000000000|648088^Basic Metabolic Panel|||20150101000000|||||||||1620^Johnson^Corey^A||||||20150101000000|||F|||||||||||20150101000000|\r\n" +
            "OBX|1|NM|GLU^Glucose Lvl|59|mg/dL|65-99^65^99|L|||F|||20150102000000|\r\n";

        final SortedMap<String, String> expectedAttributes = new TreeMap<>();

        expectedAttributes.put("MSH.1", "|");
        expectedAttributes.put("MSH.2", "^~\\&");
        expectedAttributes.put("MSH.3", "XXXXXXXX");
        expectedAttributes.put("MSH.5", "HealthProvider");
        expectedAttributes.put("MSH.9", "ORU^R01");
        expectedAttributes.put("MSH.10", "Q1111111111111111111");
        expectedAttributes.put("MSH.11", "P");
        expectedAttributes.put("MSH.12", "2.3");

        expectedAttributes.put("ORC_1.1", "NW");
        expectedAttributes.put("ORC_1.2", "987654321^EPC");
        expectedAttributes.put("ORC_1.3", "123456789^EPC");
        expectedAttributes.put("ORC_1.9", "20161003000000");
        expectedAttributes.put("ORC_1.12", "SMITH");

        expectedAttributes.put("OBR_1.1", "1");
        expectedAttributes.put("OBR_1.2", "341856649^HNAM_ORDERID");
        expectedAttributes.put("OBR_1.3", "000000000000000000");
        expectedAttributes.put("OBR_1.4", "648088^Basic Metabolic Panel");
        expectedAttributes.put("OBR_1.7", "20150101000000");
        expectedAttributes.put("OBR_1.16", "1620^Johnson^Corey^A");
        expectedAttributes.put("OBR_1.22", "20150101000000");
        expectedAttributes.put("OBR_1.25", "F");
        expectedAttributes.put("OBR_1.36", "20150101000000");

        expectedAttributes.put("OBX_1.1", "1");
        expectedAttributes.put("OBX_1.2", "NM");
        expectedAttributes.put("OBX_1.3", "GLU^Glucose Lvl");
        expectedAttributes.put("OBX_1.4", "59");
        expectedAttributes.put("OBX_1.5", "mg/dL");
        expectedAttributes.put("OBX_1.6", "65-99^65^99");
        expectedAttributes.put("OBX_1.7", "L");
        expectedAttributes.put("OBX_1.10", "F");
        expectedAttributes.put("OBX_1.13", "20150102000000");

        expectedAttributes.put("PD1.4", "1234567890^LAST^FIRST^M^^^^^NPI");

        expectedAttributes.put("PID.3", "12345^^^XYZ^MR");
        expectedAttributes.put("PID.5", "SMITH^JOHN");
        expectedAttributes.put("PID.7", "19700100");
        expectedAttributes.put("PID.8", "M");
        expectedAttributes.put("PID.18", "111111111111");
        expectedAttributes.put("PID.19", "123456789");

        runTests(message, expectedAttributes);
    }

    @Test
    public void testExtractWithSegmentNames() throws IOException {
        final String message = "MSH|^~\\&|XXXXXXXX||HealthProvider||||ORU^R01|Q1111111111111111111|P|2.3|\r\n" +
            "PID|||12345^^^XYZ^MR||SMITH^JOHN||19700100|M||||||||||111111111111|123456789|\r\n" +
            "PD1||||1234567890^LAST^FIRST^M^^^^^NPI|\r\n" +
            "ORC|NW|987654321^EPC|123456789^EPC||||||20161003000000|||SMITH\r\n" +
            "OBR|1|341856649^HNAM_ORDERID|000000000000000000|648088^Basic Metabolic Panel|||20150101000000|||||||||1620^Johnson^Corey^A||||||20150101000000|||F|||||||||||20150101000000|\r\n" +
            "OBX|1|NM|GLU^Glucose Lvl|59|mg/dL|65-99^65^99|L|||F|||20150102000000|\r\n";

        final SortedMap<String, String> expectedAttributes = new TreeMap<>();

        expectedAttributes.put("MSH.FieldSeparator", "|");
        expectedAttributes.put("MSH.EncodingCharacters", "^~\\&");
        expectedAttributes.put("MSH.SendingApplication", "XXXXXXXX");
        expectedAttributes.put("MSH.ReceivingApplication", "HealthProvider");
        expectedAttributes.put("MSH.MessageType", "ORU^R01");
        expectedAttributes.put("MSH.MessageControlID", "Q1111111111111111111");
        expectedAttributes.put("MSH.ProcessingID", "P");
        expectedAttributes.put("MSH.VersionID", "2.3");

        expectedAttributes.put("ORC_1.OrderControl", "NW");
        expectedAttributes.put("ORC_1.PlacerOrderNumber", "987654321^EPC");
        expectedAttributes.put("ORC_1.FillerOrderNumber", "123456789^EPC");
        expectedAttributes.put("ORC_1.DateTimeOfTransaction", "20161003000000");
        expectedAttributes.put("ORC_1.OrderingProvider", "SMITH");

        expectedAttributes.put("OBR_1.SetIDObservationRequest", "1");
        expectedAttributes.put("OBR_1.PlacerOrderNumber", "341856649^HNAM_ORDERID");
        expectedAttributes.put("OBR_1.FillerOrderNumber", "000000000000000000");
        expectedAttributes.put("OBR_1.UniversalServiceIdentifier", "648088^Basic Metabolic Panel");
        expectedAttributes.put("OBR_1.ObservationDateTime", "20150101000000");
        expectedAttributes.put("OBR_1.OrderingProvider", "1620^Johnson^Corey^A");
        expectedAttributes.put("OBR_1.ResultsRptStatusChngDateTime", "20150101000000");
        expectedAttributes.put("OBR_1.ResultStatus", "F");
        expectedAttributes.put("OBR_1.ScheduledDateTime", "20150101000000");

        expectedAttributes.put("OBX_1.SetIDOBX", "1");
        expectedAttributes.put("OBX_1.ValueType", "NM");
        expectedAttributes.put("OBX_1.ObservationIdentifier", "GLU^Glucose Lvl");
        expectedAttributes.put("OBX_1.ObservationSubID", "59");
        expectedAttributes.put("OBX_1.ObservationValue", "mg/dL");
        expectedAttributes.put("OBX_1.Units", "65-99^65^99");
        expectedAttributes.put("OBX_1.ReferencesRange", "L");
        expectedAttributes.put("OBX_1.NatureOfAbnormalTest", "F");
        expectedAttributes.put("OBX_1.UserDefinedAccessChecks", "20150102000000");

        expectedAttributes.put("PD1.PatientPrimaryCareProviderNameIDNo", "1234567890^LAST^FIRST^M^^^^^NPI");

        expectedAttributes.put("PID.PatientIDInternalID", "12345^^^XYZ^MR");
        expectedAttributes.put("PID.PatientName", "SMITH^JOHN");
        expectedAttributes.put("PID.DateOfBirth", "19700100");
        expectedAttributes.put("PID.Sex", "M");
        expectedAttributes.put("PID.PatientAccountNumber", "111111111111");
        expectedAttributes.put("PID.SSNNumberPatient", "123456789");

        runTests(message, expectedAttributes, true, false);
    }

    @Test
    public void testExtractWithSegmentNamesAndFields() throws IOException {
        final String message = "MSH|^~\\&|XXXXXXXX||HealthProvider||||ORU^R01|Q1111111111111111111|P|2.3|\r\n" +
            "PID|||12345^^^XYZ^MR||SMITH^JOHN||19700100|M||||||||||111111111111|123456789|\r\n" +
            "PD1||||1234567890^LAST^FIRST^M^^^^^NPI|\r\n" +
            "ORC|NW|987654321^EPC|123456789^EPC||||||20161003000000|||SMITH\r\n" +
            "OBR|1|341856649^HNAM_ORDERID|000000000000000000|648088^Basic Metabolic Panel|||20150101000000|||||||||1620^Johnson^Corey^A||||||20150101000000|||F|||||||||||20150101000000|\r\n" +
            "OBX|1|NM|GLU^Glucose Lvl|59|mg/dL|65-99^65^99|L|||F|||20150102000000|\r\n";

        final SortedMap<String, String> expectedAttributes = new TreeMap<>();

        expectedAttributes.put("MSH.FieldSeparator", "|");
        expectedAttributes.put("MSH.EncodingCharacters", "^~\\&");
        expectedAttributes.put("MSH.SendingApplication.HD.1", "XXXXXXXX");
        expectedAttributes.put("MSH.ReceivingApplication.HD.1", "HealthProvider");
        expectedAttributes.put("MSH.MessageType.CM.1", "ORU");
        expectedAttributes.put("MSH.MessageType.CM.2", "R01");
        expectedAttributes.put("MSH.MessageControlID", "Q1111111111111111111");
        expectedAttributes.put("MSH.ProcessingID.PT.1", "P");
        expectedAttributes.put("MSH.VersionID", "2.3");

        expectedAttributes.put("ORC_1.OrderControl", "NW");
        expectedAttributes.put("ORC_1.PlacerOrderNumber.EI.1", "987654321");
        expectedAttributes.put("ORC_1.PlacerOrderNumber.EI.2", "EPC");
        expectedAttributes.put("ORC_1.FillerOrderNumber.EI.1", "123456789");
        expectedAttributes.put("ORC_1.FillerOrderNumber.EI.2", "EPC");
        expectedAttributes.put("ORC_1.DateTimeOfTransaction", "20161003000000");
        expectedAttributes.put("ORC_1.OrderingProvider.XCN.1", "SMITH");

        expectedAttributes.put("OBR_1.SetIDObservationRequest", "1");
        expectedAttributes.put("OBR_1.PlacerOrderNumber.EI.1", "341856649");
        expectedAttributes.put("OBR_1.PlacerOrderNumber.EI.2", "HNAM_ORDERID");
        expectedAttributes.put("OBR_1.FillerOrderNumber.EI.1", "000000000000000000");
        expectedAttributes.put("OBR_1.UniversalServiceIdentifier.CE.1", "648088");
        expectedAttributes.put("OBR_1.UniversalServiceIdentifier.CE.2", "Basic Metabolic Panel");
        expectedAttributes.put("OBR_1.ObservationDateTime", "20150101000000");
        expectedAttributes.put("OBR_1.OrderingProvider.XCN.1", "1620");
        expectedAttributes.put("OBR_1.OrderingProvider.XCN.2", "Johnson");
        expectedAttributes.put("OBR_1.OrderingProvider.XCN.3", "Corey");
        expectedAttributes.put("OBR_1.OrderingProvider.XCN.4", "A");
        expectedAttributes.put("OBR_1.ResultsRptStatusChngDateTime", "20150101000000");
        expectedAttributes.put("OBR_1.ResultStatus", "F");
        expectedAttributes.put("OBR_1.ScheduledDateTime", "20150101000000");

        expectedAttributes.put("OBX_1.SetIDOBX", "1");
        expectedAttributes.put("OBX_1.ValueType", "NM");
        expectedAttributes.put("OBX_1.ObservationIdentifier.CE.1", "GLU");
        expectedAttributes.put("OBX_1.ObservationIdentifier.CE.2", "Glucose Lvl");
        expectedAttributes.put("OBX_1.ObservationSubID", "59");
        expectedAttributes.put("OBX_1.ObservationValue", "mg/dL");
        expectedAttributes.put("OBX_1.Units.CE.1", "65-99");
        expectedAttributes.put("OBX_1.Units.CE.2", "65");
        expectedAttributes.put("OBX_1.Units.CE.3", "99");
        expectedAttributes.put("OBX_1.ReferencesRange", "L");
        expectedAttributes.put("OBX_1.NatureOfAbnormalTest", "F");
        expectedAttributes.put("OBX_1.UserDefinedAccessChecks", "20150102000000");

        expectedAttributes.put("PD1.PatientPrimaryCareProviderNameIDNo.XCN.1", "1234567890");
        expectedAttributes.put("PD1.PatientPrimaryCareProviderNameIDNo.XCN.2", "LAST");
        expectedAttributes.put("PD1.PatientPrimaryCareProviderNameIDNo.XCN.3", "FIRST");
        expectedAttributes.put("PD1.PatientPrimaryCareProviderNameIDNo.XCN.4", "M");
        expectedAttributes.put("PD1.PatientPrimaryCareProviderNameIDNo.XCN.9", "NPI");

        expectedAttributes.put("PID.PatientIDInternalID.CX.1", "12345");
        expectedAttributes.put("PID.PatientIDInternalID.CX.4", "XYZ");
        expectedAttributes.put("PID.PatientIDInternalID.CX.5", "MR");
        expectedAttributes.put("PID.PatientName.XPN.1", "SMITH");
        expectedAttributes.put("PID.PatientName.XPN.2", "JOHN");
        expectedAttributes.put("PID.DateOfBirth", "19700100");
        expectedAttributes.put("PID.Sex", "M");
        expectedAttributes.put("PID.PatientAccountNumber.CX.1", "111111111111");
        expectedAttributes.put("PID.SSNNumberPatient", "123456789");

        runTests(message, expectedAttributes, true, true);
    }

}
