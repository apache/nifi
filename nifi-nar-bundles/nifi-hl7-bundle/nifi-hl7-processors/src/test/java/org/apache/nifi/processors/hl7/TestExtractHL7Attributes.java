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

    public static final String TEST_INPUT_RECORD =
        "MSH|^~\\&|XXXXXXXX||HealthProvider||||ORU^R01|Q1111111111111111111|P|2.3|\r\n" +
        "PID|||12345^^^XYZ^MR||SMITH^JOHN||19700100|M||||||||||111111111111|123456789|\r\n" +
        "PD1||||1234567890^LAST^FIRST^M^^^^^NPI|\r\n" +
        "ORC|NW|987654321^EPC|123456789^EPC||||||20161003000000|||SMITH\r\n" +
        "OBR|1|341856649^HNAM_ORDERID|000000000000000000|648088^Basic Metabolic Panel|||20150101000000|||||||||1620^Johnson^Corey^A||||||20150101000000|||F|||||||||||20150101000000|\r\n" +
        "OBX|1|NM|GLU^Glucose Lvl|59|mg/dL|65-99^65^99|L|||F|||20150102000000|\r\n";

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

    @Test
    public void test1714219() {
        // Message from http://hl7api.sourceforge.net hapi-test/src/test/java/ca/uhn/hl7v2/parser/ParserTest.java
        final String message = "MSH|^~\\&|Send App|Send Fac|Rec App|Rec Fac|20070504141816||ORM^O01||P|2.2\r" +
            "PID|||12345678||Lastname^^INI^^PREFIX||19340207|F|||Street 15^^S GRAVENHAGE^^2551HL^NEDERLAND|||||||||||||||NL\r" +
            "ORC|NW|8100088345^ORDERNR||LN1||C|^^^20070504080000||20070504141816|||0^Doctor\r" +
            "OBR|1|8100088345^ORDERNR||ADR^Something||||||||||||0^Doctor\r" +
            "OBX|1|ST|ADR^Something||item1^item2^item3^^item5||||||F\r";

        final SortedMap<String, String> expectedAttributes = new TreeMap<>();

        expectedAttributes.put("MSH.1", "|");
        expectedAttributes.put("MSH.2", "^~\\&");
        expectedAttributes.put("MSH.3", "Send App");
        expectedAttributes.put("MSH.4", "Send Fac");
        expectedAttributes.put("MSH.5", "Rec App");
        expectedAttributes.put("MSH.6", "Rec Fac");
        expectedAttributes.put("MSH.7", "20070504141816");
        expectedAttributes.put("MSH.9", "ORM^O01");
        expectedAttributes.put("MSH.11", "P");
        expectedAttributes.put("MSH.12", "2.2");

        expectedAttributes.put("PID.3", "12345678");
        expectedAttributes.put("PID.5", "Lastname^^INI^^PREFIX");
        expectedAttributes.put("PID.7", "19340207");
        expectedAttributes.put("PID.8", "F");
        expectedAttributes.put("PID.11", "Street 15^^S GRAVENHAGE^^2551HL^NEDERLAND");
        expectedAttributes.put("PID.26", "NL");

        expectedAttributes.put("ORC_1.1", "NW");
        expectedAttributes.put("ORC_1.2", "8100088345^ORDERNR");
        expectedAttributes.put("ORC_1.4", "LN1");
        expectedAttributes.put("ORC_1.6", "C");
        expectedAttributes.put("ORC_1.7", "^^^20070504080000");
        expectedAttributes.put("ORC_1.9", "20070504141816");
        expectedAttributes.put("ORC_1.12", "0^Doctor");

        expectedAttributes.put("OBR.1", "1");
        expectedAttributes.put("OBR.2", "8100088345^ORDERNR");
        expectedAttributes.put("OBR.4", "ADR^Something");
        expectedAttributes.put("OBR.16", "0^Doctor");

        expectedAttributes.put("OBX.1", "1");
        expectedAttributes.put("OBX.2", "ST");
        expectedAttributes.put("OBX.3", "ADR^Something");
        expectedAttributes.put("OBX.5", "item1^item2^item3^^item5");
        expectedAttributes.put("OBX.11", "F");

        runTests(message, expectedAttributes);
    }

    @Test
    public void testNPACExample() {
        // Message from http://hl7api.sourceforge.net hapi-osgi-test/src/test/resources/ca/uhn/hl7v2/util/messages.txt
        final String message = "MSH|^~\\&|RADIS1||DMCRES||1994050216163360||ORU^R01|1994050216163360|D|2.2|964||AL|AL\r" +
            "MSA|AA|msgCtrlId\r" +
            "PID|||N00803||RADIOLOGY^INPATIENT^SIX||19520606|F||A||||||||003555||\r" +
            "PV1||I|N77^7714^01|||||||OB|\r" +
            "OBR|1|003555.0015.001^DMCRES|0000000566^RADIS1|37953^CT CHEST^L|||199405021545|||||||||||||0000763||||A999|P||||||R/O TUMOR|202300^BAKER^MARK^E|||01^LOCHLEAR, JUDY|\r" +
            "OBX||TX|FIND^FINDINGS^L|1|This is a test on 05/02/94.|\r" +
            "OBX||TX|FIND^FINDINGS^L|2|This is a test for the CRR.|\r" +
            "OBX||TX|FIND^FINDINGS^L|3|This is a test result to generate multiple obr's to check the cost|\r" +
            "OBX||TX|FIND^FINDINGS^L|4|display for multiple exams.|\r" +
            "OBX||TX|FIND^FINDINGS^L|5|APPROVING MD:|\r" +
            "OBR|2|^DMCRES|0000000567^RADIS1|37956^CT ABDOMEN^L|||199405021550|||||||||||||0000763|||||P||||||R/O TUMOR|202300^BAKER^MARK^E|||01^LOCHLEAR, JUDY|\r" +
            "OBR|3|^DMCRES|0000000568^RADIS1|37881^CT PELVIS (LIMITED)^L|||199405021551|||||||||||||0000763|||||P||||||R/O TUMOR|202300^BAKER^MARK^E|||01^LOCHLEAR, JUDY|";

        final SortedMap<String, String> expectedAttributes = new TreeMap<>();

        expectedAttributes.put("MSH.1", "|");
        expectedAttributes.put("MSH.2", "^~\\&");
        expectedAttributes.put("MSH.3", "RADIS1");
        expectedAttributes.put("MSH.5", "DMCRES");
        expectedAttributes.put("MSH.7", "1994050216163360");
        expectedAttributes.put("MSH.9", "ORU^R01");
        expectedAttributes.put("MSH.10", "1994050216163360");
        expectedAttributes.put("MSH.11", "D");
        expectedAttributes.put("MSH.12", "2.2");
        expectedAttributes.put("MSH.13", "964");
        expectedAttributes.put("MSH.15", "AL");
        expectedAttributes.put("MSH.16", "AL");

        expectedAttributes.put("MSA.1", "AA");
        expectedAttributes.put("MSA.2", "msgCtrlId");

        expectedAttributes.put("PID.3", "N00803");
        expectedAttributes.put("PID.5", "RADIOLOGY^INPATIENT^SIX");
        expectedAttributes.put("PID.7", "19520606");
        expectedAttributes.put("PID.8", "F");
        expectedAttributes.put("PID.10", "A");
        expectedAttributes.put("PID.18", "003555");

        expectedAttributes.put("PV1.2", "I");
        expectedAttributes.put("PV1.3", "N77^7714^01");
        expectedAttributes.put("PV1.10", "OB");

        expectedAttributes.put("OBR_1.1", "1");
        expectedAttributes.put("OBR_1.2", "003555.0015.001^DMCRES");
        expectedAttributes.put("OBR_1.3", "0000000566^RADIS1");
        expectedAttributes.put("OBR_1.4", "37953^CT CHEST^L");
        expectedAttributes.put("OBR_1.7", "199405021545");
        expectedAttributes.put("OBR_1.20", "0000763");
        expectedAttributes.put("OBR_1.24", "A999");
        expectedAttributes.put("OBR_1.25", "P");
        expectedAttributes.put("OBR_1.31", "R/O TUMOR");
        expectedAttributes.put("OBR_1.32", "202300^BAKER^MARK^E");
        expectedAttributes.put("OBR_1.35", "01^LOCHLEAR, JUDY");

        expectedAttributes.put("OBX_1.2", "TX");
        expectedAttributes.put("OBX_1.3", "FIND^FINDINGS^L");
        expectedAttributes.put("OBX_1.4", "1");
        expectedAttributes.put("OBX_1.5", "This is a test on 05/02/94.");

        expectedAttributes.put("OBX_2.2", "TX");
        expectedAttributes.put("OBX_2.3", "FIND^FINDINGS^L");
        expectedAttributes.put("OBX_2.4", "2");
        expectedAttributes.put("OBX_2.5", "This is a test for the CRR.");

        expectedAttributes.put("OBX_3.2", "TX");
        expectedAttributes.put("OBX_3.3", "FIND^FINDINGS^L");
        expectedAttributes.put("OBX_3.4", "3");
        expectedAttributes.put("OBX_3.5", "This is a test result to generate multiple obr's to check the cost");

        expectedAttributes.put("OBX_4.2", "TX");
        expectedAttributes.put("OBX_4.3", "FIND^FINDINGS^L");
        expectedAttributes.put("OBX_4.4", "4");
        expectedAttributes.put("OBX_4.5", "display for multiple exams.");

        expectedAttributes.put("OBX_5.2", "TX");
        expectedAttributes.put("OBX_5.3", "FIND^FINDINGS^L");
        expectedAttributes.put("OBX_5.4", "5");
        expectedAttributes.put("OBX_5.5", "APPROVING MD:");

        expectedAttributes.put("OBR_2.1", "2");
        expectedAttributes.put("OBR_2.2", "^DMCRES");
        expectedAttributes.put("OBR_2.3", "0000000567^RADIS1");
        expectedAttributes.put("OBR_2.4", "37956^CT ABDOMEN^L");
        expectedAttributes.put("OBR_2.7", "199405021550");
        expectedAttributes.put("OBR_2.20", "0000763");
        expectedAttributes.put("OBR_2.25", "P");
        expectedAttributes.put("OBR_2.31", "R/O TUMOR");
        expectedAttributes.put("OBR_2.32", "202300^BAKER^MARK^E");
        expectedAttributes.put("OBR_2.35", "01^LOCHLEAR, JUDY");

        expectedAttributes.put("OBR_3.1", "3");
        expectedAttributes.put("OBR_3.2", "^DMCRES");
        expectedAttributes.put("OBR_3.3", "0000000568^RADIS1");
        expectedAttributes.put("OBR_3.4", "37881^CT PELVIS (LIMITED)^L");
        expectedAttributes.put("OBR_3.7", "199405021551");
        expectedAttributes.put("OBR_3.20", "0000763");
        expectedAttributes.put("OBR_3.25", "P");
        expectedAttributes.put("OBR_3.31", "R/O TUMOR");
        expectedAttributes.put("OBR_3.32", "202300^BAKER^MARK^E");
        expectedAttributes.put("OBR_3.35", "01^LOCHLEAR, JUDY");

        runTests(message, expectedAttributes);
    }

    @Test
    public void testADT() {
        // Message from http://hl7api.sourceforge.net hapi-osgi-test/src/test/resources/ca/uhn/hl7v2/parser/tests/adt_a03.txt
        final String message = "MSH|^~\\&|IRIS|SANTER|AMB_R|SANTER|200803051508||ADT^A03|263206|P|2.5\r" +
            "EVN||200803051509||||200803031508\r" +
            "PID|||5520255^^^PK^PK~ZZZZZZ83M64Z148R^^^CF^CF~ZZZZZZ83M64Z148R^^^SSN^SSN^^20070103^99991231~^^^^TEAM||ZZZ^ZZZ||19830824|F||||||||||||||||||||||N\r" +
            "PV1||I|6402DH^^^^^^^^MED. 1 - ONCOLOGIA^^OSPEDALE MAGGIORE DI LODI&LODI|||^^^^^^^^^^OSPEDALE MAGGIORE DI LODI&LODI|13936^TEST^TEST" +
                "||||||||||5068^TEST2^TEST2||2008003369||||||||||||||||||||||||||200803031508\r" +
            "PR1|1||1111^Mastoplastica|Protesi|20090224|02|";

        final SortedMap<String, String> expectedAttributes = new TreeMap<>();

        expectedAttributes.put("MSH.1", "|");
        expectedAttributes.put("MSH.2", "^~\\&");
        expectedAttributes.put("MSH.3", "IRIS");
        expectedAttributes.put("MSH.4", "SANTER");
        expectedAttributes.put("MSH.5", "AMB_R");
        expectedAttributes.put("MSH.6", "SANTER");
        expectedAttributes.put("MSH.7", "200803051508");
        expectedAttributes.put("MSH.9", "ADT^A03");
        expectedAttributes.put("MSH.10", "263206");
        expectedAttributes.put("MSH.11", "P");
        expectedAttributes.put("MSH.12", "2.5");

        expectedAttributes.put("EVN.2", "200803051509");
        expectedAttributes.put("EVN.6", "200803031508");

        // NOTE: PID.3 should be the following but there's a long-standing bug where additional fields don't get rendered here
        //expectedAttributes.put("PID.3", "5520255^^^PK^PK~ZZZZZZ83M64Z148R^^^CF^CF~ZZZZZZ83M64Z148R^^^SSN^SSN^^20070103^99991231~^^^^TEAM");

        expectedAttributes.put("PID.3", "5520255^^^PK^PK");
        expectedAttributes.put("PID.5", "ZZZ^ZZZ");
        expectedAttributes.put("PID.7", "19830824");
        expectedAttributes.put("PID.8", "F");
        expectedAttributes.put("PID.30", "N");

        expectedAttributes.put("PV1.2", "I");
        expectedAttributes.put("PV1.3", "6402DH^^^^^^^^MED. 1 - ONCOLOGIA^^OSPEDALE MAGGIORE DI LODI&LODI");
        expectedAttributes.put("PV1.6", "^^^^^^^^^^OSPEDALE MAGGIORE DI LODI&LODI");
        expectedAttributes.put("PV1.7", "13936^TEST^TEST");
        expectedAttributes.put("PV1.17", "5068^TEST2^TEST2");
        expectedAttributes.put("PV1.19", "2008003369");
        expectedAttributes.put("PV1.45", "200803031508");

        expectedAttributes.put("PR1_1.1", "1");
        expectedAttributes.put("PR1_1.3", "1111^Mastoplastica");
        expectedAttributes.put("PR1_1.4", "Protesi");
        expectedAttributes.put("PR1_1.5", "20090224");
        expectedAttributes.put("PR1_1.6", "02");

        runTests(message, expectedAttributes);
    }
}
