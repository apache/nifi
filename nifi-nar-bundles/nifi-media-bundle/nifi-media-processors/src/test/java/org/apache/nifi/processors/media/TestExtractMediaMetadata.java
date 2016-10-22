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
package org.apache.nifi.processors.media;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestExtractMediaMetadata {

    @Test
    public void testProperties() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        ProcessContext context = runner.getProcessContext();
        Map<PropertyDescriptor, String> propertyValues = context.getProperties();
        assertEquals(4, propertyValues.size());
    }

    @Test
    public void testRelationships() {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        ProcessContext context = runner.getProcessContext();
        Set<Relationship> relationships = context.getAvailableRelationships();
        assertEquals(2, relationships.size());
        assertTrue(relationships.contains(ExtractMediaMetadata.SUCCESS));
        assertTrue(relationships.contains(ExtractMediaMetadata.FAILURE));
    }

    @Test
    public void testTextBytes() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_FILTER, "");
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "txt.");
        runner.assertValid();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test1.txt");
        runner.enqueue("test1".getBytes(), attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile0 = successFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeEquals("filename", "test1.txt");
        flowFile0.assertAttributeExists("txt.Content-Type");
        assertTrue(flowFile0.getAttribute("txt.Content-Type").startsWith("text/plain"));
        flowFile0.assertAttributeExists("txt.X-Parsed-By");
        assertTrue(flowFile0.getAttribute("txt.X-Parsed-By").contains("org.apache.tika.parser.DefaultParser"));
        assertTrue(flowFile0.getAttribute("txt.X-Parsed-By").contains("org.apache.tika.parser.txt.TXTParser"));
        flowFile0.assertAttributeExists("txt.Content-Encoding");
        flowFile0.assertAttributeEquals("txt.Content-Encoding", "ISO-8859-1");
        flowFile0.assertContentEquals("test1".getBytes("UTF-8"));
    }

    @Test
    public void testProvenance() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_FILTER, "");
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "txt.");
        runner.assertValid();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "test1.txt");
        runner.enqueue("test1".getBytes(), attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        final List<ProvenanceEventRecord> events = runner.getProvenanceEvents();
        assertEquals(1, events.size());

        final ProvenanceEventRecord event = events.get(0);
        assertEquals(ExtractMediaMetadata.class.getSimpleName(), event.getComponentType());
        assertEquals("media attributes extracted", event.getDetails());
        assertEquals(ProvenanceEventType.ATTRIBUTES_MODIFIED, event.getEventType());
    }

    @Test
    public void testNoFlowFile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_FILTER, "");
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "txt.");
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 0);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);
    }

    @Test
    public void testTextFile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_FILTER, "");
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "txt.");
        runner.assertValid();

        runner.enqueue(new File("target/test-classes/textFile.txt").toPath());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile0 = successFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeEquals("filename", "textFile.txt");
        flowFile0.assertAttributeExists("txt.Content-Type");
        assertTrue(flowFile0.getAttribute("txt.Content-Type").startsWith("text/plain"));
        flowFile0.assertAttributeExists("txt.X-Parsed-By");
        assertTrue(flowFile0.getAttribute("txt.X-Parsed-By").contains("org.apache.tika.parser.DefaultParser"));
        assertTrue(flowFile0.getAttribute("txt.X-Parsed-By").contains("org.apache.tika.parser.txt.TXTParser"));
        flowFile0.assertAttributeExists("txt.Content-Encoding");
        flowFile0.assertAttributeEquals("txt.Content-Encoding", "ISO-8859-1");
        flowFile0.assertContentEquals("This file is not an image and is used for testing the image metadata extractor.".getBytes("UTF-8"));
    }

    @Test
    public void testBigTextFile() throws IOException {
        File textFile = new File("target/test-classes/textFileBig.txt");

        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "txt.");
        runner.assertValid();

        runner.enqueue(textFile.toPath());
        runner.run(2);

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile0 = successFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeEquals("filename", "textFileBig.txt");
        flowFile0.assertAttributeExists("txt.Content-Type");
        assertTrue(flowFile0.getAttribute("txt.Content-Type").startsWith("text/plain"));
        flowFile0.assertAttributeExists("txt.X-Parsed-By");
        assertTrue(flowFile0.getAttribute("txt.X-Parsed-By").contains("org.apache.tika.parser.DefaultParser"));
        assertTrue(flowFile0.getAttribute("txt.X-Parsed-By").contains("org.apache.tika.parser.txt.TXTParser"));
        flowFile0.assertAttributeExists("txt.Content-Encoding");
        flowFile0.assertAttributeEquals("txt.Content-Encoding", "ISO-8859-1");
        assertEquals(flowFile0.getSize(), textFile.length());
    }

    @Test
    public void testJunkBytes() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_FILTER, "");
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "junk.");
        runner.assertValid();

        final Map<String, String> attrs = new HashMap<>();
        attrs.put("filename", "junk");
        Random random = new Random();
        byte[] bytes = new byte[2048];
        random.nextBytes(bytes);
        runner.enqueue(bytes, attrs);
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile0 = successFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeEquals("filename", "junk");
        flowFile0.assertAttributeExists("junk.Content-Type");
        assertTrue(flowFile0.getAttribute("junk.Content-Type").startsWith("application/octet-stream"));
        flowFile0.assertAttributeExists("junk.X-Parsed-By");
        assertTrue(flowFile0.getAttribute("junk.X-Parsed-By").contains("org.apache.tika.parser.EmptyParser"));
        flowFile0.assertContentEquals(bytes);
    }

    @Test
    public void testMetadataKeyFilter() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_FILTER, "(X-Parsed.*)");
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "txt.");
        runner.assertValid();

        runner.enqueue(new File("target/test-classes/textFile.txt").toPath());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile0 = successFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeEquals("filename", "textFile.txt");
        flowFile0.assertAttributeExists("txt.X-Parsed-By");
        assertTrue(flowFile0.getAttribute("txt.X-Parsed-By").contains("org.apache.tika.parser.DefaultParser"));
        assertTrue(flowFile0.getAttribute("txt.X-Parsed-By").contains("org.apache.tika.parser.txt.TXTParser"));
        flowFile0.assertAttributeNotExists("txt.Content-Encoding");
    }

    @Test
    public void testMetadataKeyPrefix() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.assertValid();

        runner.enqueue(new File("target/test-classes/textFile.txt").toPath());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile0 = successFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeExists("X-Parsed-By");

        runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "txt.");
        runner.assertValid();

        runner.enqueue(new File("target/test-classes/textFile.txt").toPath());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        flowFile0 = successFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeExists("txt.X-Parsed-By");
    }

    @Test
    public void testMaxAttributes() throws IOException {
        TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "txt.");
        runner.assertValid();

        runner.enqueue(new File("target/test-classes/textFile.txt").toPath());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        List<MockFlowFile> successFiles0 = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile0 = successFiles0.get(0);
        int fileAttrCount0 = 0;
        for (Map.Entry attr : flowFile0.getAttributes().entrySet()) {
            if (attr.getKey().toString().startsWith("txt.")) {
                fileAttrCount0++;
            }
        }
        assertTrue(fileAttrCount0 > 1);

        runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.MAX_NUMBER_OF_ATTRIBUTES, Integer.toString(fileAttrCount0 - 1));
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "txt.");
        runner.assertValid();

        runner.enqueue(new File("target/test-classes/textFile.txt").toPath());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile1 = successFiles.get(0);
        int fileAttrCount1 = 0;
        for (Map.Entry attr : flowFile1.getAttributes().entrySet()) {
            if (attr.getKey().toString().startsWith("txt.")) {
                fileAttrCount1++;
            }
        }
        assertEquals(fileAttrCount0, fileAttrCount1 + 1);
    }

    @Test
    public void testBmp() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "bmp.");
        runner.assertValid();

        runner.enqueue(new File("target/test-classes/16color-10x10.bmp").toPath());
        runner.run(2);

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile0 = successFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeEquals("filename", "16color-10x10.bmp");
        flowFile0.assertAttributeExists("bmp.Content-Type");
        flowFile0.assertAttributeEquals("bmp.Content-Type", "image/x-ms-bmp");
        flowFile0.assertAttributeExists("bmp.X-Parsed-By");
        assertTrue(flowFile0.getAttribute("bmp.X-Parsed-By").contains("org.apache.tika.parser.DefaultParser"));
        // assertTrue(flowFile0.getAttribute("bmp.X-Parsed-By").contains("org.apache.tika.parser.image.ImageParser"));
        flowFile0.assertAttributeExists("bmp.height");
        flowFile0.assertAttributeEquals("bmp.height", "10");
        flowFile0.assertAttributeExists("bmp.width");
        flowFile0.assertAttributeEquals("bmp.width", "10");
    }

    @Test
    public void testJpg() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "jpg.");
        runner.assertValid();

        runner.enqueue(new File("target/test-classes/simple.jpg").toPath());
        runner.run(2);

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile0 = successFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeEquals("filename", "simple.jpg");
        flowFile0.assertAttributeExists("jpg.Content-Type");
        flowFile0.assertAttributeEquals("jpg.Content-Type", "image/jpeg");
        flowFile0.assertAttributeExists("jpg.X-Parsed-By");
        assertTrue(flowFile0.getAttribute("jpg.X-Parsed-By").contains("org.apache.tika.parser.DefaultParser"));
        // assertTrue(flowFile0.getAttribute("jpg.X-Parsed-By").contains("org.apache.tika.parser.jpeg.JpegParser"));
    }

    @Test
    public void testWav() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_FILTER, "");
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "wav.");
        runner.assertValid();

        runner.enqueue(new File("target/test-classes/testWAV.wav").toPath());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile0 = successFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeEquals("filename", "testWAV.wav");
        flowFile0.assertAttributeExists("wav.Content-Type");
        assertTrue(flowFile0.getAttribute("wav.Content-Type").startsWith("audio/x-wav"));
        flowFile0.assertAttributeExists("wav.X-Parsed-By");
        assertTrue(flowFile0.getAttribute("wav.X-Parsed-By").contains("org.apache.tika.parser.DefaultParser"));
        assertTrue(flowFile0.getAttribute("wav.X-Parsed-By").contains("org.apache.tika.parser.audio.AudioParser"));
        flowFile0.assertAttributeExists("wav.encoding");
        flowFile0.assertAttributeEquals("wav.encoding", "PCM_SIGNED");
    }

    @Test
    public void testOgg() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_FILTER, "");
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "ogg.");
        runner.assertValid();

        runner.enqueue(new File("target/test-classes/testVORBIS.ogg").toPath());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile0 = successFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeEquals("filename", "testVORBIS.ogg");
        flowFile0.assertAttributeExists("ogg.Content-Type");
        assertTrue(flowFile0.getAttribute("ogg.Content-Type").startsWith("audio/vorbis"));
        flowFile0.assertAttributeExists("ogg.X-Parsed-By");
        assertTrue(flowFile0.getAttribute("ogg.X-Parsed-By").contains("org.apache.tika.parser.DefaultParser"));
        assertTrue(flowFile0.getAttribute("ogg.X-Parsed-By").contains("org.gagravarr.tika.VorbisParser"));
    }

    @Test
    public void testOggCorruptFails() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_FILTER, "");
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "ogg.");
        runner.assertValid();

        runner.enqueue(new File("target/test-classes/testVORBIS-corrupt.ogg").toPath());
        runner.run(2);

        runner.assertTransferCount(ExtractMediaMetadata.SUCCESS, 0);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 1);

        final List<MockFlowFile> failureFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.FAILURE);
        MockFlowFile flowFile0 = failureFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeEquals("filename", "testVORBIS-corrupt.ogg");
        flowFile0.assertAttributeNotExists("ogg.Content-Type");
        flowFile0.assertAttributeNotExists("ogg.X-Parsed-By");
    }

    @Test
    public void testMp3() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ExtractMediaMetadata());
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_FILTER, "");
        runner.setProperty(ExtractMediaMetadata.METADATA_KEY_PREFIX, "mp3.");
        runner.assertValid();

        runner.enqueue(new File("target/test-classes/testMP3id3v1.mp3").toPath());
        runner.run();

        runner.assertAllFlowFilesTransferred(ExtractMediaMetadata.SUCCESS, 1);
        runner.assertTransferCount(ExtractMediaMetadata.FAILURE, 0);

        final List<MockFlowFile> successFiles = runner.getFlowFilesForRelationship(ExtractMediaMetadata.SUCCESS);
        MockFlowFile flowFile0 = successFiles.get(0);
        flowFile0.assertAttributeExists("filename");
        flowFile0.assertAttributeEquals("filename", "testMP3id3v1.mp3");
        flowFile0.assertAttributeExists("mp3.Content-Type");
        assertTrue(flowFile0.getAttribute("mp3.Content-Type").startsWith("audio/mpeg"));
        flowFile0.assertAttributeExists("mp3.X-Parsed-By");
        assertTrue(flowFile0.getAttribute("mp3.X-Parsed-By").contains("org.apache.tika.parser.DefaultParser"));
        assertTrue(flowFile0.getAttribute("mp3.X-Parsed-By").contains("org.apache.tika.parser.mp3.Mp3Parser"));
        flowFile0.assertAttributeExists("mp3.title");
        flowFile0.assertAttributeEquals("mp3.title", "Test Title");
    }

}