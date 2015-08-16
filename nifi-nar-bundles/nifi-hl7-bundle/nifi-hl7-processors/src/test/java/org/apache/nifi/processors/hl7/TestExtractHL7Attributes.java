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
        final TestRunner runner = TestRunners.newTestRunner(ExtractHL7Attributes.class);
        runner.enqueue(Paths.get("src/test/resources/hypoglycemia.hl7"));

        runner.run();
        runner.assertAllFlowFilesTransferred(ExtractHL7Attributes.REL_SUCCESS, 1);

        final MockFlowFile out = runner.getFlowFilesForRelationship(ExtractHL7Attributes.REL_SUCCESS).get(0);
        final SortedMap<String, String> sortedAttrs = new TreeMap<>(out.getAttributes());

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
