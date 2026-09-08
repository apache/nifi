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

import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestSegmentContent {

    @Test
    public void test() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new SegmentContent());
        testRunner.setProperty(SegmentContent.SIZE, "4 B");

        testRunner.enqueue(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, Map.of(CoreAttributes.FILENAME.key(), "data.bin"));
        testRunner.run();

        testRunner.assertTransferCount(SegmentContent.REL_ORIGINAL, 1);
        final MockFlowFile originalFlowFile = testRunner.getFlowFilesForRelationship(SegmentContent.REL_ORIGINAL).get(0);
        originalFlowFile.assertAttributeExists(SegmentContent.FRAGMENT_ID);
        originalFlowFile.assertAttributeEquals(SegmentContent.FRAGMENT_COUNT, "3");
        originalFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "data.bin");

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(SegmentContent.REL_SEGMENTS);
        assertEquals(3, flowFiles.size());

        final MockFlowFile out1 = flowFiles.get(0);
        final MockFlowFile out2 = flowFiles.get(1);
        final MockFlowFile out3 = flowFiles.get(2);

        out1.assertContentEquals(new byte[]{1, 2, 3, 4});
        out2.assertContentEquals(new byte[]{5, 6, 7, 8});
        out3.assertContentEquals(new byte[]{9});

        assertSegmentKeepsOriginalFilename(out1, "data.bin");
        assertSegmentKeepsOriginalFilename(out2, "data.bin");
        assertSegmentKeepsOriginalFilename(out3, "data.bin");
        out1.assertAttributeEquals(SegmentContent.FRAGMENT_INDEX, "1");
        out2.assertAttributeEquals(SegmentContent.FRAGMENT_INDEX, "2");
        out3.assertAttributeEquals(SegmentContent.FRAGMENT_INDEX, "3");
        out1.assertAttributeEquals(SegmentContent.FRAGMENT_COUNT, "3");
        assertEquals(out1.getAttribute(SegmentContent.FRAGMENT_ID), out2.getAttribute(SegmentContent.FRAGMENT_ID));
        assertEquals(out1.getAttribute(SegmentContent.FRAGMENT_ID), originalFlowFile.getAttribute(SegmentContent.FRAGMENT_ID));
    }

    @Test
    public void testTransferSmall() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new SegmentContent());
        testRunner.setProperty(SegmentContent.SIZE, "4 KB");

        testRunner.enqueue(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, Map.of(CoreAttributes.FILENAME.key(), "small.bin"));
        testRunner.run();

        testRunner.assertTransferCount(SegmentContent.REL_ORIGINAL, 1);
        final MockFlowFile originalFlowFile = testRunner.getFlowFilesForRelationship(SegmentContent.REL_ORIGINAL).get(0);
        originalFlowFile.assertAttributeExists(SegmentContent.FRAGMENT_ID);
        originalFlowFile.assertAttributeEquals(SegmentContent.FRAGMENT_COUNT, "1");
        originalFlowFile.assertAttributeEquals(CoreAttributes.FILENAME.key(), "small.bin");
        originalFlowFile.assertAttributeEquals(SegmentContent.SEGMENT_ORIGINAL_FILENAME, "small.bin");

        testRunner.assertTransferCount(SegmentContent.REL_SEGMENTS, 1);
        final MockFlowFile out1 = testRunner.getFlowFilesForRelationship(SegmentContent.REL_SEGMENTS).get(0);
        out1.assertContentEquals(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
        assertSegmentKeepsOriginalFilename(out1, "small.bin");
        out1.assertAttributeEquals(SegmentContent.FRAGMENT_INDEX, "1");
        out1.assertAttributeEquals(SegmentContent.FRAGMENT_COUNT, "1");
    }

    @Test
    public void testExpressionLanguage() throws IOException {
        final TestRunner testRunner = TestRunners.newTestRunner(new SegmentContent());
        Map<String, String> attributes = new HashMap<>();
        attributes.put("segmentSize", "4 B");
        testRunner.setProperty(SegmentContent.SIZE, "${segmentSize}");
        testRunner.assertValid();

        testRunner.enqueue(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, attributes);
        testRunner.run();

        testRunner.assertTransferCount(SegmentContent.REL_ORIGINAL, 1);
        final MockFlowFile originalFlowFile = testRunner.getFlowFilesForRelationship(SegmentContent.REL_ORIGINAL).get(0);
        originalFlowFile.assertAttributeExists(SegmentContent.FRAGMENT_ID);
        originalFlowFile.assertAttributeEquals(SegmentContent.FRAGMENT_COUNT, "3");

        final List<MockFlowFile> flowFiles = testRunner.getFlowFilesForRelationship(SegmentContent.REL_SEGMENTS);
        assertEquals(3, flowFiles.size());

        final MockFlowFile out1 = flowFiles.get(0);
        final MockFlowFile out2 = flowFiles.get(1);
        final MockFlowFile out3 = flowFiles.get(2);

        out1.assertContentEquals(new byte[]{1, 2, 3, 4});
        out2.assertContentEquals(new byte[]{5, 6, 7, 8});
        out3.assertContentEquals(new byte[]{9});
    }

    @Test
    public void testWritesAttributesDocumentActualFilenameBehavior() {
        final WritesAttribute[] documented = SegmentContent.class.getAnnotation(WritesAttributes.class).value();
        final List<String> names = Arrays.stream(documented).map(WritesAttribute::attribute).toList();

        names.forEach(name -> assertEquals(name, name.strip(), "Documented attribute name has surrounding whitespace: '" + name + "'"));
        assertEquals(1, names.stream().filter(SegmentContent.SEGMENT_ORIGINAL_FILENAME::equals).count());

        Arrays.stream(documented)
                .map(WritesAttribute::description)
                .forEach(description -> assertFalse(description.contains("will be updated"),
                        "Documentation still claims filename is rewritten: " + description));
    }

    private static void assertSegmentKeepsOriginalFilename(final MockFlowFile segment, final String originalFilename) {
        segment.assertAttributeEquals(CoreAttributes.FILENAME.key(), originalFilename);
        segment.assertAttributeEquals(SegmentContent.SEGMENT_ORIGINAL_FILENAME, originalFilename);
    }
}
