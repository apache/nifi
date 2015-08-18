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
package org.apache.nifi.processors.image;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class ExtractImageMetadataTest {
    private static String BMP_HEADER = "BMP Header.";
    private static String JPEG_HEADER = "JPEG.";
    private static String GIF_HEADER = "GIF Header.";
    private static String PNG_HEADER = "PNG.";

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ExtractImageMetadata.class);
    }

    @Test
    public void testFailedExtraction() throws IOException {
        MockFlowFile flowFile = verifyTestRunnerFlow("src/test/resources/notImage.txt", ExtractImageMetadata.FAILURE,"1000");
    }

    @Test
    public void testExtractJPG() throws IOException {
        MockFlowFile flowFile = verifyTestRunnerFlow("src/test/resources/simple.jpg", ExtractImageMetadata.SUCCESS,"1000");
        Map<String, String> attributes = flowFile.getAttributes();

        assertEquals("800 pixels", attributes.get(JPEG_HEADER + "Image Width"));
        assertEquals("600 pixels", attributes.get(JPEG_HEADER + "Image Height"));
        assertEquals("8 bits", attributes.get(JPEG_HEADER + "Data Precision"));
        assertEquals("Baseline", attributes.get(JPEG_HEADER + "Compression Type"));
        assertEquals("3", attributes.get(JPEG_HEADER + "Number of Components"));
        assertEquals("Y component: Quantization table 0, Sampling factors 2 horiz/2 vert",
                attributes.get(JPEG_HEADER + "Component 1"));
        assertEquals("Cb component: Quantization table 1, Sampling factors 1 horiz/1 vert",
                attributes.get(JPEG_HEADER + "Component 2"));
        assertEquals("Cr component: Quantization table 1, Sampling factors 1 horiz/1 vert",
                attributes.get(JPEG_HEADER + "Component 3"));
    }

    @Test
    public void testExtractGIF() throws IOException {
        MockFlowFile flowFile = verifyTestRunnerFlow(
                "src/test/resources/photoshop-8x12-32colors-alpha.gif", ExtractImageMetadata.SUCCESS,"1000");
        Map<String, String> attributes = flowFile.getAttributes();

        assertEquals("8", attributes.get(GIF_HEADER + "Image Width"));
        assertEquals("12", attributes.get(GIF_HEADER + "Image Height"));
        assertEquals("true", attributes.get(GIF_HEADER + "Has Global Color Table"));
        assertEquals("32", attributes.get(GIF_HEADER + "Color Table Size"));
        assertEquals("8", attributes.get(GIF_HEADER + "Transparent Color Index"));
        assertEquals("89a", attributes.get(GIF_HEADER + "GIF Format Version"));
        assertEquals("5", attributes.get(GIF_HEADER + "Bits per Pixel"));
        assertEquals("false", attributes.get(GIF_HEADER + "Is Color Table Sorted"));
    }

    @Test
     public void testExtractPNG() throws IOException {
        MockFlowFile flowFile = verifyTestRunnerFlow("src/test/resources/mspaint-8x10.png", ExtractImageMetadata.SUCCESS, "1000");
        Map<String, String> attributes = flowFile.getAttributes();

        assertEquals("8", attributes.get(PNG_HEADER + "Image Width"));
        assertEquals("12", attributes.get(PNG_HEADER + "Image Height"));
        assertEquals("0.45455", attributes.get(PNG_HEADER + "Image Gamma"));
        assertEquals("Deflate", attributes.get(PNG_HEADER + "Compression Type"));
        assertEquals("No Interlace", attributes.get(PNG_HEADER + "Interlace Method"));
        assertEquals("Perceptual", attributes.get(PNG_HEADER + "sRGB Rendering Intent"));
        assertEquals("Adaptive", attributes.get(PNG_HEADER + "Filter Method"));
        assertEquals("8", attributes.get(PNG_HEADER + "Bits Per Sample"));
        assertEquals("True Color", attributes.get(PNG_HEADER + "Color Type"));
    }
    @Test
     public void testExtractBMP() throws IOException {
        MockFlowFile flowFile = verifyTestRunnerFlow("src/test/resources/16color-10x10.bmp", ExtractImageMetadata.SUCCESS, "1000");
        Map<String, String> attributes = flowFile.getAttributes();

        assertEquals("10", attributes.get(BMP_HEADER+"Image Width"));
        assertEquals("10", attributes.get(BMP_HEADER+"Image Height"));
        assertEquals("4", attributes.get(BMP_HEADER+"Bits Per Pixel"));
        assertEquals("None", attributes.get(BMP_HEADER+"Compression"));
        assertEquals("0", attributes.get(BMP_HEADER+"X Pixels per Meter"));
        assertEquals("0", attributes.get(BMP_HEADER+"Y Pixels per Meter"));
        assertEquals("0", attributes.get(BMP_HEADER+"Palette Colour Count"));
        assertEquals("0", attributes.get(BMP_HEADER+"Important Colour Count"));
        assertEquals("1", attributes.get(BMP_HEADER+"Planes"));
        assertEquals("40", attributes.get(BMP_HEADER+"Header Size"));
    }
    @Test
    public void testExtractLimitedAttributesBMP() throws IOException {
        MockFlowFile flowFile = verifyTestRunnerFlow("src/test/resources/16color-10x10.bmp", ExtractImageMetadata.SUCCESS, "5");
        Map<String, String> attributes = flowFile.getAttributes();

        assertEquals("10", attributes.get(BMP_HEADER+"Image Width"));
        assertEquals("10", attributes.get(BMP_HEADER+"Image Height"));
        assertEquals("4", attributes.get(BMP_HEADER+"Bits Per Pixel"));
        assertEquals("1", attributes.get(BMP_HEADER+"Planes"));
        assertEquals("40", attributes.get(BMP_HEADER+"Header Size"));


        assertNull(attributes.get(BMP_HEADER + "Compression"));
        assertNull(attributes.get(BMP_HEADER + "X Pixels per Meter"));
        assertNull(attributes.get(BMP_HEADER + "Y Pixels per Meter"));
        assertNull(attributes.get(BMP_HEADER + "Palette Colour Count"));
        assertNull(attributes.get(BMP_HEADER + "Important Colour Count"));
    }

    public MockFlowFile verifyTestRunnerFlow(String pathStr,Relationship rel, String max) throws IOException {
        Path path = Paths.get(pathStr);
        testRunner.enqueue(path);
        testRunner.setProperty(ExtractImageMetadata.MaxAttributes, max);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(rel, 1);


        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(rel).get(0);
        testRunner.assertQueueEmpty();

        testRunner.enqueue(flowFile);
        testRunner.clearTransferState();
        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(rel, 1);

        flowFile = testRunner.getFlowFilesForRelationship(rel).get(0);
        flowFile.assertContentEquals(new File(pathStr));
        return  flowFile;
    }
}
