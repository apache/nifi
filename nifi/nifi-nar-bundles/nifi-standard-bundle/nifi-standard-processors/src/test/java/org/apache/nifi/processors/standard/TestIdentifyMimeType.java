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

import org.apache.nifi.processors.standard.IdentifyMimeType;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.Test;

public class TestIdentifyMimeType {

    @Test
    public void testFiles() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new IdentifyMimeType());

        final File dir = new File("src/test/resources/TestIdentifyMimeType");
        final File[] files = dir.listFiles();
        int fileCount = 0;
        for (final File file : files) {
            if (file.isDirectory()) {
                continue;
            }

            runner.enqueue(file.toPath());
            fileCount++;
        }

        runner.setThreadCount(1);
        runner.run(fileCount);

        runner.assertAllFlowFilesTransferred(IdentifyMimeType.REL_SUCCESS, fileCount);

        final Map<String, String> expectedMimeTypes = new HashMap<>();
        expectedMimeTypes.put("1.7z", "application/x-7z-compressed");
        expectedMimeTypes.put("1.mdb", "application/msaccess");
        expectedMimeTypes.put("1.txt.bz2", "application/bzip2");
        expectedMimeTypes.put("1.txt.gz", "application/gzip");
        expectedMimeTypes.put("1.zip", "application/zip");
        expectedMimeTypes.put("bgBannerFoot.png", "image/png");
        expectedMimeTypes.put("blueBtnBg.jpg", "image/jpg");
        expectedMimeTypes.put("1.pdf", "application/pdf");
        expectedMimeTypes.put("grid.gif", "image/gif");
        expectedMimeTypes.put("1.tar", "application/octet-stream"); //wrong ID without IDENTIFY_TAR
        expectedMimeTypes.put("1.jar", "application/java-archive");
        expectedMimeTypes.put("1.xml", "application/xml");
        expectedMimeTypes.put("flowfilev3", "application/flowfile-v3");
        expectedMimeTypes.put("flowfilev1.tar", "application/tar"); //wrong ID without IDENTIFY_TAR

        final List<MockFlowFile> filesOut = runner.getFlowFilesForRelationship(IdentifyMimeType.REL_SUCCESS);
        for (final MockFlowFile file : filesOut) {
            final String filename = file.getAttribute(CoreAttributes.FILENAME.key());
            final String mimeType = file.getAttribute(CoreAttributes.MIME_TYPE.key());
            final String expected = expectedMimeTypes.get(filename);
            assertEquals("Expected " + file + " to have MIME Type " + expected + ", but it was " + mimeType, expected, mimeType);
        }
    }

    @Test
    public void testFilesWithIdentifyTarAndZip() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new IdentifyMimeType());
        runner.setProperty(IdentifyMimeType.IDENTIFY_TAR.getName(), "true");
        runner.setProperty(IdentifyMimeType.IDENTIFY_ZIP.getName(), "true");

        final File dir = new File("src/test/resources/TestIdentifyMimeType");
        final File[] files = dir.listFiles();
        int fileCount = 0;
        for (final File file : files) {
            if (file.isDirectory()) {
                continue;
            }

            runner.enqueue(file.toPath());
            fileCount++;
        }

        runner.setThreadCount(4);
        runner.run(fileCount);

        runner.assertAllFlowFilesTransferred(IdentifyMimeType.REL_SUCCESS, fileCount);

        final Map<String, String> expectedMimeTypes = new HashMap<>();
        expectedMimeTypes.put("1.7z", "application/x-7z-compressed");
        expectedMimeTypes.put("1.mdb", "application/msaccess");
        expectedMimeTypes.put("1.txt.bz2", "application/bzip2");
        expectedMimeTypes.put("1.txt.gz", "application/gzip");
        expectedMimeTypes.put("1.zip", "application/zip");
        expectedMimeTypes.put("bgBannerFoot.png", "image/png");
        expectedMimeTypes.put("blueBtnBg.jpg", "image/jpg");
        expectedMimeTypes.put("1.pdf", "application/pdf");
        expectedMimeTypes.put("grid.gif", "image/gif");
        expectedMimeTypes.put("1.tar", "application/tar");
        expectedMimeTypes.put("1.jar", "application/java-archive");
        expectedMimeTypes.put("1.xml", "application/xml");
        expectedMimeTypes.put("flowfilev3", "application/flowfile-v3");
        expectedMimeTypes.put("flowfilev1.tar", "application/flowfile-v1");

        final List<MockFlowFile> filesOut = runner.getFlowFilesForRelationship(IdentifyMimeType.REL_SUCCESS);
        for (final MockFlowFile file : filesOut) {
            final String filename = file.getAttribute(CoreAttributes.FILENAME.key());
            final String mimeType = file.getAttribute(CoreAttributes.MIME_TYPE.key());
            final String expected = expectedMimeTypes.get(filename);
            assertEquals("Expected " + file + " to have MIME Type " + expected + ", but it was " + mimeType, expected, mimeType);
        }
    }

}
