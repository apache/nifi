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

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.flowfile.attributes.StandardFlowFileMediaType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestIdentifyMimeType {

    private static final String CONFIG_FILE = "src/test/resources/TestIdentifyMimeType/.customConfig.xml";
    private static final String CONFIG_BODY =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
            "<mime-info>\n" +
            "  <mime-type type=\"custom/abcd\">\n" +
            "    <magic priority=\"50\">\n" +
            "      <match value=\"abcd\" type=\"string\" offset=\"0\"/>\n" +
            "    </magic>\n" +
            "    <glob pattern=\"*.abcd\" />\n" +
            "  </mime-type>\n" +
            "  <mime-type type=\"image/png\">\n" +
            "    <acronym>PNG</acronym>\n" +
            "    <_comment>Portable Network Graphics</_comment>\n" +
            "    <magic priority=\"50\">\n" +
            "      <match value=\"\\x89PNG\\x0d\\x0a\\x1a\\x0a\" type=\"string\" offset=\"0\"/>\n" +
            "    </magic>\n" +
            "    <glob pattern=\"*.customPng\"/>\n" +
            "  </mime-type>\n" +
            "</mime-info>";

    @Test
    public void testFiles() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new IdentifyMimeType());

        final File dir = new File("src/test/resources/TestIdentifyMimeType");
        final File[] files = dir.listFiles((ldir,name)-> name != null && !name.startsWith("."));
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

        final Map<String, String> expectedMimeTypes = getCommonExpectedMimeTypes();
        expectedMimeTypes.put("bgBannerFoot.png", "image/png");
        expectedMimeTypes.put("blueBtnBg.jpg", "image/jpeg");
        expectedMimeTypes.put("grid.gif", "image/gif");
        expectedMimeTypes.put("2.custom", "text/plain");

        final Map<String, String> expectedExtensions = getCommonExpectedExtensions();
        expectedExtensions.put("bgBannerFoot.png", ".png");
        expectedExtensions.put("blueBtnBg.jpg", ".jpg");
        expectedExtensions.put("grid.gif", ".gif");
        expectedExtensions.put("2.custom", ".txt");

        final Map<String, String> expectedCharsets = getCommonExpectedCharsets();
        expectedCharsets.put("bgBannerFoot.png", null);
        expectedCharsets.put("blueBtnBg.jpg", null);
        expectedCharsets.put("grid.gif", null);
        expectedCharsets.put("2.custom", "ISO-8859-1");

        final List<MockFlowFile> filesOut = runner.getFlowFilesForRelationship(IdentifyMimeType.REL_SUCCESS);
        for (final MockFlowFile file : filesOut) {
            final String filename = file.getAttribute(CoreAttributes.FILENAME.key());
            final String mimeType = file.getAttribute(CoreAttributes.MIME_TYPE.key());
            final String expected = expectedMimeTypes.get(filename);

            final String extension = file.getAttribute("mime.extension");
            final String expectedExtension = expectedExtensions.get(filename);

            final String charset = file.getAttribute("mime.charset");
            final String expectedCharset = expectedCharsets.get(filename);

            assertEquals(expected, mimeType, "Expected " + file + " to have MIME Type \"" + expected + "\", but it was \"" + mimeType + "\"");
            assertEquals(expectedExtension, extension, "Expected " + file + " to have extension \"" + expectedExtension + "\", but it was \"" + extension + "\"");
            assertEquals(expectedCharset, charset, "Expected " + file + " to have charset \"" + expectedCharset + "\", but it was \"" + charset + "\"");
        }
    }

    @Test
    public void testIgnoreFileName() throws Exception {
        final TestRunner runner = TestRunners.newTestRunner(new IdentifyMimeType());
        runner.setProperty(IdentifyMimeType.USE_FILENAME_IN_DETECTION, "false");

        runner.enqueue(Paths.get("src/test/resources/TestIdentifyMimeType/fake.csv"));
        runner.run();

        runner.assertAllFlowFilesTransferred(IdentifyMimeType.REL_SUCCESS, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(IdentifyMimeType.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("mime.extension", ".txt");
        flowFile.assertAttributeEquals("mime.type", "text/plain");
    }

    @Test
    public void testReplaceWithConfigBody() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new IdentifyMimeType());


        final File dir = new File("src/test/resources/TestIdentifyMimeType");
        final File[] files = dir.listFiles((ldir,name)-> name != null && !name.startsWith("."));
        int fileCount = 0;
        for (final File file : files) {
            if (file.isDirectory()) {
                continue;
            }

            runner.enqueue(file.toPath());
            fileCount++;
        }

        runner.setProperty(IdentifyMimeType.CONFIG_STRATEGY, IdentifyMimeType.REPLACE);
        runner.setProperty(IdentifyMimeType.MIME_CONFIG_BODY, CONFIG_BODY);

        runner.setThreadCount(1);
        runner.run(fileCount);


        runner.assertAllFlowFilesTransferred(IdentifyMimeType.REL_SUCCESS, fileCount);

        final Map<String, String> expectedMimeTypes = new HashMap<>();
        expectedMimeTypes.put("1.7z", "application/octet-stream");
        expectedMimeTypes.put("1.mdb", "application/octet-stream");
        expectedMimeTypes.put("1.txt", "text/plain");
        expectedMimeTypes.put("1.csv", "text/plain");
        expectedMimeTypes.put("1.txt.bz2", "application/octet-stream");
        expectedMimeTypes.put("1.txt.gz", "application/octet-stream");
        expectedMimeTypes.put("1.zip", "application/octet-stream");
        expectedMimeTypes.put("bgBannerFoot.png", "image/png");
        expectedMimeTypes.put("blueBtnBg.jpg", "application/octet-stream");
        expectedMimeTypes.put("1.pdf", "application/octet-stream");
        expectedMimeTypes.put("grid.gif", "application/octet-stream");
        expectedMimeTypes.put("1.tar", "application/octet-stream");
        expectedMimeTypes.put("1.tar.gz", "application/octet-stream");
        expectedMimeTypes.put("1.jar", "application/octet-stream");
        expectedMimeTypes.put("1.xml", "text/plain");
        expectedMimeTypes.put("1.xhtml", "text/plain");
        expectedMimeTypes.put("flowfilev3", "application/octet-stream");
        expectedMimeTypes.put("flowfilev3WithXhtml", "application/octet-stream");
        expectedMimeTypes.put("flowfilev1.tar", "application/octet-stream");
        expectedMimeTypes.put("fake.csv", "text/plain");
        expectedMimeTypes.put("2.custom", "custom/abcd");
        expectedMimeTypes.put("charset-utf-8.txt", "text/plain");

        final Map<String, String> expectedExtensions = new HashMap<>();
        expectedExtensions.put("1.7z", "");
        expectedExtensions.put("1.mdb", "");
        expectedExtensions.put("1.txt", "");
        expectedExtensions.put("1.csv", "");
        expectedExtensions.put("1.txt.bz2", "");
        expectedExtensions.put("1.txt.gz", "");
        expectedExtensions.put("1.zip", "");
        expectedExtensions.put("bgBannerFoot.png", ".customPng");
        expectedExtensions.put("blueBtnBg.jpg", "");
        expectedExtensions.put("1.pdf", "");
        expectedExtensions.put("grid.gif", "");
        expectedExtensions.put("1.tar", "");
        expectedExtensions.put("1.tar.gz", "");
        expectedExtensions.put("1.jar", "");
        expectedExtensions.put("1.xml", "");
        expectedExtensions.put("1.xhtml", "");
        expectedExtensions.put("flowfilev3", "");
        expectedExtensions.put("flowfilev3WithXhtml", "");
        expectedExtensions.put("flowfilev1.tar", "");
        expectedExtensions.put("fake.csv", "");
        expectedExtensions.put("2.custom", ".abcd");
        expectedExtensions.put("charset-utf-8.txt", "");

        final List<MockFlowFile> filesOut = runner.getFlowFilesForRelationship(IdentifyMimeType.REL_SUCCESS);
        for (final MockFlowFile file : filesOut) {
            final String filename = file.getAttribute(CoreAttributes.FILENAME.key());
            final String mimeType = file.getAttribute(CoreAttributes.MIME_TYPE.key());
            final String expected = expectedMimeTypes.get(filename);

            final String extension = file.getAttribute("mime.extension");
            final String expectedExtension = expectedExtensions.get(filename);

            assertEquals(expected, mimeType, "Expected " + file + " to have MIME Type " + expected + ", but it was " + mimeType);
            assertEquals(expectedExtension, extension, "Expected " + file + " to have extension " + expectedExtension + ", but it was " + extension);
        }
    }

    @Test
    public void testReplaceWithConfigFile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new IdentifyMimeType());


        final File dir = new File("src/test/resources/TestIdentifyMimeType");
        final File[] files = dir.listFiles((ldir,name)-> name != null && !name.startsWith("."));
        int fileCount = 0;
        for (final File file : files) {
            if (file.isDirectory()) {
                continue;
            }

            runner.enqueue(file.toPath());
            fileCount++;
        }

        runner.setProperty(IdentifyMimeType.CONFIG_STRATEGY, IdentifyMimeType.REPLACE);
        runner.setProperty(IdentifyMimeType.MIME_CONFIG_FILE, CONFIG_FILE);

        runner.setThreadCount(1);
        runner.run(fileCount);


        runner.assertAllFlowFilesTransferred(IdentifyMimeType.REL_SUCCESS, fileCount);

        final Map<String, String> expectedMimeTypes = new HashMap<>();
        expectedMimeTypes.put("1.7z", "application/octet-stream");
        expectedMimeTypes.put("1.mdb", "application/octet-stream");
        expectedMimeTypes.put("1.txt", "text/plain");
        expectedMimeTypes.put("1.csv", "text/plain");
        expectedMimeTypes.put("1.txt.bz2", "application/octet-stream");
        expectedMimeTypes.put("1.txt.gz", "application/octet-stream");
        expectedMimeTypes.put("1.zip", "application/octet-stream");
        expectedMimeTypes.put("bgBannerFoot.png", "my/png");
        expectedMimeTypes.put("blueBtnBg.jpg", "my/jpeg");
        expectedMimeTypes.put("1.pdf", "application/octet-stream");
        expectedMimeTypes.put("grid.gif", "my/gif");
        expectedMimeTypes.put("1.tar", "application/octet-stream");
        expectedMimeTypes.put("1.tar.gz", "application/octet-stream");
        expectedMimeTypes.put("1.jar", "application/octet-stream");
        expectedMimeTypes.put("1.xml", "text/plain");
        expectedMimeTypes.put("1.xhtml", "text/plain");
        expectedMimeTypes.put("flowfilev3", "application/octet-stream");
        expectedMimeTypes.put("flowfilev3WithXhtml", "application/octet-stream");
        expectedMimeTypes.put("flowfilev1.tar", "application/octet-stream");
        expectedMimeTypes.put("fake.csv", "text/plain");
        expectedMimeTypes.put("2.custom", "text/plain");
        expectedMimeTypes.put("charset-utf-8.txt", "text/plain");

        final Map<String, String> expectedExtensions = new HashMap<>();
        expectedExtensions.put("1.7z", "");
        expectedExtensions.put("1.mdb", "");
        expectedExtensions.put("1.txt", "");
        expectedExtensions.put("1.csv", "");
        expectedExtensions.put("1.txt.bz2", "");
        expectedExtensions.put("1.txt.gz", "");
        expectedExtensions.put("1.zip", "");
        expectedExtensions.put("bgBannerFoot.png", ".mypng");
        expectedExtensions.put("blueBtnBg.jpg", ".myjpg");
        expectedExtensions.put("1.pdf", "");
        expectedExtensions.put("grid.gif", ".mygif");
        expectedExtensions.put("1.tar", "");
        expectedExtensions.put("1.tar.gz", "");
        expectedExtensions.put("1.jar", "");
        expectedExtensions.put("1.xml", "");
        expectedExtensions.put("1.xhtml", "");
        expectedExtensions.put("flowfilev3", "");
        expectedExtensions.put("flowfilev3WithXhtml", "");
        expectedExtensions.put("flowfilev1.tar", "");
        expectedExtensions.put("fake.csv", "");
        expectedExtensions.put("2.custom", "");
        expectedExtensions.put("charset-utf-8.txt", "");

        final List<MockFlowFile> filesOut = runner.getFlowFilesForRelationship(IdentifyMimeType.REL_SUCCESS);
        for (final MockFlowFile file : filesOut) {
            final String filename = file.getAttribute(CoreAttributes.FILENAME.key());
            final String mimeType = file.getAttribute(CoreAttributes.MIME_TYPE.key());
            final String expected = expectedMimeTypes.get(filename);

            final String extension = file.getAttribute("mime.extension");
            final String expectedExtension = expectedExtensions.get(filename);

            assertEquals(expected, mimeType, "Expected " + file + " to have MIME Type " + expected + ", but it was " + mimeType);
            assertEquals(expectedExtension, extension, "Expected " + file + " to have extension " + expectedExtension + ", but it was " + extension);
        }
    }

    @Test
    public void testMergeWithConfigBody() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new IdentifyMimeType());

        final File dir = new File("src/test/resources/TestIdentifyMimeType");
        final File[] files = dir.listFiles((ldir,name)-> name != null && !name.startsWith(".") && new File(ldir, name).isFile());
        int fileCount = 0;
        for (final File file : files) {
            runner.enqueue(file.toPath());
            fileCount++;
        }

        runner.setProperty(IdentifyMimeType.CONFIG_STRATEGY, IdentifyMimeType.MERGE);
        runner.setProperty(IdentifyMimeType.MIME_CONFIG_BODY, CONFIG_BODY);

        runner.setThreadCount(1);
        runner.run(fileCount);

        runner.assertAllFlowFilesTransferred(IdentifyMimeType.REL_SUCCESS, fileCount);

        final Map<String, String> expectedMimeTypes = getCommonExpectedMimeTypes();
        expectedMimeTypes.put("bgBannerFoot.png", "image/png");
        expectedMimeTypes.put("blueBtnBg.jpg", "image/jpeg");
        expectedMimeTypes.put("grid.gif", "image/gif");
        expectedMimeTypes.put("2.custom", "custom/abcd");

        final Map<String, String> expectedExtensions = getCommonExpectedExtensions();
        expectedExtensions.put("bgBannerFoot.png", ".customPng");
        expectedExtensions.put("blueBtnBg.jpg", ".jpg");
        expectedExtensions.put("grid.gif", ".gif");
        expectedExtensions.put("2.custom", ".abcd");

        final List<MockFlowFile> filesOut = runner.getFlowFilesForRelationship(IdentifyMimeType.REL_SUCCESS);
        for (final MockFlowFile file : filesOut) {
            final String filename = file.getAttribute(CoreAttributes.FILENAME.key());
            final String mimeType = file.getAttribute(CoreAttributes.MIME_TYPE.key());
            final String expected = expectedMimeTypes.get(filename);

            final String extension = file.getAttribute("mime.extension");
            final String expectedExtension = expectedExtensions.get(filename);

            assertEquals(expected, mimeType, "Expected " + file + " to have MIME Type " + expected + ", but it was " + mimeType);
            assertEquals(expectedExtension, extension, "Expected " + file + " to have extension " + expectedExtension + ", but it was " + extension);
        }
    }

    @Test
    public void testMergeWithConfigFile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new IdentifyMimeType());

        final File dir = new File("src/test/resources/TestIdentifyMimeType");
        final File[] files = dir.listFiles((ldir,name)-> name != null && !name.startsWith(".") && new File(ldir, name).isFile());
        int fileCount = 0;
        for (final File file : files) {
            runner.enqueue(file.toPath());
            fileCount++;
        }

        runner.setProperty(IdentifyMimeType.CONFIG_STRATEGY, IdentifyMimeType.MERGE);
        runner.setProperty(IdentifyMimeType.MIME_CONFIG_FILE, CONFIG_FILE);

        runner.setThreadCount(1);
        runner.run(fileCount);

        runner.assertAllFlowFilesTransferred(IdentifyMimeType.REL_SUCCESS, fileCount);

        final Map<String, String> expectedMimeTypes = getCommonExpectedMimeTypes();
        expectedMimeTypes.put("bgBannerFoot.png", "my/png");
        expectedMimeTypes.put("blueBtnBg.jpg", "my/jpeg");
        expectedMimeTypes.put("grid.gif", "my/gif");
        expectedMimeTypes.put("2.custom", "text/plain");

        final Map<String, String> expectedExtensions = getCommonExpectedExtensions();
        expectedExtensions.put("bgBannerFoot.png", ".mypng");
        expectedExtensions.put("blueBtnBg.jpg", ".myjpg");
        expectedExtensions.put("grid.gif", ".mygif");
        expectedExtensions.put("2.custom", ".txt");

        final List<MockFlowFile> filesOut = runner.getFlowFilesForRelationship(IdentifyMimeType.REL_SUCCESS);
        for (final MockFlowFile file : filesOut) {
            final String filename = file.getAttribute(CoreAttributes.FILENAME.key());
            final String mimeType = file.getAttribute(CoreAttributes.MIME_TYPE.key());
            final String expected = expectedMimeTypes.get(filename);

            final String extension = file.getAttribute("mime.extension");
            final String expectedExtension = expectedExtensions.get(filename);

            assertEquals(expected, mimeType, "Expected " + file + " to have MIME Type " + expected + ", but it was " + mimeType);
            assertEquals(expectedExtension, extension, "Expected " + file + " to have extension " + expectedExtension + ", but it was " + extension);
        }
    }

    @Test
    public void testOnlyOneCustomMimeConfigSpecified() {
        final TestRunner runner = TestRunners.newTestRunner(new IdentifyMimeType());

        runner.setProperty(IdentifyMimeType.CONFIG_STRATEGY, IdentifyMimeType.REPLACE);
        runner.setProperty(IdentifyMimeType.MIME_CONFIG_FILE, CONFIG_FILE);

        String configBody = "foo";
        runner.setProperty(IdentifyMimeType.MIME_CONFIG_BODY, configBody);

        runner.setThreadCount(1);
        assertThrows(AssertionError.class, () -> {
            runner.run();
        });
    }

    @Test
    public void testNoCustomMimeConfigSpecified() {
        final TestRunner runner = TestRunners.newTestRunner(new IdentifyMimeType());

        runner.setProperty(IdentifyMimeType.CONFIG_STRATEGY, IdentifyMimeType.REPLACE);

        runner.setThreadCount(1);
        assertThrows(AssertionError.class, () -> {
            runner.run();
        });
    }

    private Map<String, String> getCommonExpectedMimeTypes() {
        Map<String, String> expectedMimeTypes = new HashMap<>();

        expectedMimeTypes.put("1.7z", "application/x-7z-compressed");
        expectedMimeTypes.put("1.mdb", "application/x-msaccess");
        expectedMimeTypes.put("1.txt", "text/plain");
        expectedMimeTypes.put("1.csv", "text/csv");
        expectedMimeTypes.put("1.txt.bz2", "application/x-bzip2");
        expectedMimeTypes.put("1.txt.gz", "application/gzip");
        expectedMimeTypes.put("1.zip", "application/zip");
        expectedMimeTypes.put("1.pdf", "application/pdf");
        expectedMimeTypes.put("1.tar", "application/x-tar");
        expectedMimeTypes.put("1.tar.gz", "application/gzip");
        expectedMimeTypes.put("1.jar", "application/java-archive");
        expectedMimeTypes.put("1.xml", "application/xml");
        expectedMimeTypes.put("1.xhtml", "application/xhtml+xml");
        expectedMimeTypes.put("flowfilev3", StandardFlowFileMediaType.VERSION_3.getMediaType());
        expectedMimeTypes.put("flowfilev3WithXhtml", StandardFlowFileMediaType.VERSION_3.getMediaType());
        expectedMimeTypes.put("flowfilev1.tar", StandardFlowFileMediaType.VERSION_1.getMediaType());
        expectedMimeTypes.put("fake.csv", "text/csv");
        expectedMimeTypes.put("charset-utf-8.txt", "text/plain");

        return expectedMimeTypes;
    }

    private Map<String, String> getCommonExpectedExtensions() {
        Map<String, String> expectedExtensions = new HashMap<>();

        expectedExtensions.put("1.7z", ".7z");
        expectedExtensions.put("1.mdb", ".mdb");
        expectedExtensions.put("1.txt", ".txt");
        expectedExtensions.put("1.csv", ".csv");
        expectedExtensions.put("1.txt.bz2", ".bz2");
        expectedExtensions.put("1.txt.gz", ".gz");
        expectedExtensions.put("1.zip", ".zip");
        expectedExtensions.put("1.pdf", ".pdf");
        expectedExtensions.put("1.tar", ".tar");
        expectedExtensions.put("1.tar.gz", ".gz");
        expectedExtensions.put("1.jar", ".jar");
        expectedExtensions.put("1.xml", ".xml");
        expectedExtensions.put("1.xhtml", ".xhtml");
        expectedExtensions.put("flowfilev3", "");
        expectedExtensions.put("flowfilev3WithXhtml", "");
        expectedExtensions.put("flowfilev1.tar", "");
        expectedExtensions.put("fake.csv", ".csv");
        expectedExtensions.put("charset-utf-8.txt", ".txt");

        return expectedExtensions;
    }

    private static Map<String, String> getCommonExpectedCharsets() {
        final Map<String, String> expectedCharsets = new HashMap<>();
        expectedCharsets.put("1.7z", null);
        expectedCharsets.put("1.mdb", null);
        expectedCharsets.put("1.txt", "ISO-8859-1");
        expectedCharsets.put("1.csv", "ISO-8859-1");
        expectedCharsets.put("1.txt.bz2", null);
        expectedCharsets.put("1.txt.gz", null);
        expectedCharsets.put("1.zip", null);
        expectedCharsets.put("1.pdf", null);
        expectedCharsets.put("1.tar", null);
        expectedCharsets.put("1.tar.gz", null);
        expectedCharsets.put("1.jar", null);
        expectedCharsets.put("1.xml", null);
        expectedCharsets.put("1.xhtml", null);
        expectedCharsets.put("flowfilev3", null);
        expectedCharsets.put("flowfilev3WithXhtml", null);
        expectedCharsets.put("flowfilev1.tar", null);
        expectedCharsets.put("fake.csv", "ISO-8859-1");
        expectedCharsets.put("charset-utf-8.txt", "UTF-8");
        return expectedCharsets;
    }
}
