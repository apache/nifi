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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class TestExtractHL7Attributes {

    @BeforeClass
    public static void setup() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi", "DEBUG");
    }

    @Test
    public void testExtract() throws IOException {
        final SortedMap<String, String> expectedAttributes = new TreeMap<>();
        // MSH.1 and MSH.2 could be escaped, but it's not clear which is right
        expectedAttributes.put("MSH.1", "|");
        expectedAttributes.put("MSH.2", "^~\\&");
        expectedAttributes.put("MSH.3", "XXXXXXXX");
        expectedAttributes.put("MSH.5", "HealthProvider");
        expectedAttributes.put("MSH.9", "ORU^R01");
        expectedAttributes.put("MSH.10", "Q1111111111111111111");
        expectedAttributes.put("MSH.11", "P");
        expectedAttributes.put("MSH.12", "2.3");
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

        final TestRunner runner = TestRunners.newTestRunner(ExtractHL7Attributes.class);
        runner.enqueue(Paths.get("src/test/resources/hypoglycemia.hl7"));

        runner.run();
        runner.assertAllFlowFilesTransferred(ExtractHL7Attributes.REL_SUCCESS, 1);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ExtractHL7Attributes.REL_SUCCESS).get(0);
        final SortedMap<String, String> sortedAttrs = new TreeMap<>(out.getAttributes());

        for (final Map.Entry<String, String> entry : expectedAttributes.entrySet()) {
            final String key = entry.getKey();
            final String expected = entry.getValue();
            final String actual = sortedAttrs.get(key);
            Assert.assertEquals(key + " segment values do not match", expected, actual);
        }

        int mshSegmentCount = 0;
        int obrSegmentCount = 0;
        int obxSegmentCount = 0;
        int pidSegmentCount = 0;

        for (final Map.Entry<String, String> entry : sortedAttrs.entrySet()) {
            final String entryKey = entry.getKey();
            if (entryKey.startsWith("MSH")) {
                mshSegmentCount++;
                continue;
            } else if (entryKey.startsWith("OBR")) {
                obrSegmentCount++;
                continue;
            } else if (entryKey.startsWith("OBX")) {
                obxSegmentCount++;
                continue;
            } else if (entryKey.startsWith("PID")) {
                pidSegmentCount++;
                continue;
            }
        }

        Assert.assertEquals("Did not have the proper number of MSH segments", 8, mshSegmentCount);
        Assert.assertEquals("Did not have the proper number of OBR segments", 9, obrSegmentCount);
        Assert.assertEquals("Did not have the proper number of OBX segments", 9, obxSegmentCount);
        Assert.assertEquals("Did not have the proper number of PID segments", 6, pidSegmentCount);
    }
}
