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

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class TestResizeImage {

    @Test
    public void testResize() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ResizeImage());
        runner.setProperty(ResizeImage.IMAGE_HEIGHT, "64");
        runner.setProperty(ResizeImage.IMAGE_WIDTH, "64");
        runner.setProperty(ResizeImage.SCALING_ALGORITHM, ResizeImage.RESIZE_SMOOTH);

        runner.enqueue(Paths.get("src/test/resources/simple.jpg"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ResizeImage.REL_SUCCESS, 1);
        MockFlowFile mff = runner.getFlowFilesForRelationship(ResizeImage.REL_SUCCESS).get(0);
        byte[] data = mff.toByteArray();

        BufferedImage img = ImageIO.read(new ByteArrayInputStream(data));
        assertEquals(64, img.getWidth());
        assertEquals(64, img.getHeight());
        File out = new File("target/smooth.jpg");
        ImageIO.write(img, "JPG", out);

        runner.clearTransferState();

        runner.setProperty(ResizeImage.SCALING_ALGORITHM, ResizeImage.RESIZE_FAST);

        runner.enqueue(Paths.get("src/test/resources/simple.jpg"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ResizeImage.REL_SUCCESS, 1);
        mff = runner.getFlowFilesForRelationship(ResizeImage.REL_SUCCESS).get(0);
        data = mff.toByteArray();

        img = ImageIO.read(new ByteArrayInputStream(data));
        assertEquals(64, img.getWidth());
        assertEquals(64, img.getHeight());
        out = new File("target/fast.jpg");
        ImageIO.write(img, "JPG", out);
    }

    @Test
    public void testEnlarge() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ResizeImage());
        runner.setProperty(ResizeImage.IMAGE_HEIGHT, "600");
        runner.setProperty(ResizeImage.IMAGE_WIDTH, "600");
        runner.setProperty(ResizeImage.SCALING_ALGORITHM, ResizeImage.RESIZE_SMOOTH);

        runner.enqueue(Paths.get("src/test/resources/photoshop-8x12-32colors-alpha.gif"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ResizeImage.REL_SUCCESS, 1);
        MockFlowFile mff = runner.getFlowFilesForRelationship(ResizeImage.REL_SUCCESS).get(0);
        byte[] data = mff.toByteArray();

        BufferedImage img = ImageIO.read(new ByteArrayInputStream(data));
        assertEquals(600, img.getWidth());
        assertEquals(600, img.getHeight());
        File out = new File("target/enlarge.png");
        ImageIO.write(img, "PNG", out);
    }


    @Test
    public void testResizePNG() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(new ResizeImage());
        runner.setProperty(ResizeImage.IMAGE_HEIGHT, "64");
        runner.setProperty(ResizeImage.IMAGE_WIDTH, "64");
        runner.setProperty(ResizeImage.SCALING_ALGORITHM, ResizeImage.RESIZE_SMOOTH);

        runner.enqueue(Paths.get("src/test/resources/mspaint-8x10.png"));
        runner.run();

        runner.assertAllFlowFilesTransferred(ResizeImage.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(ResizeImage.REL_SUCCESS).get(0);
        final byte[] data = mff.toByteArray();

        final BufferedImage img = ImageIO.read(new ByteArrayInputStream(data));
        assertEquals(64, img.getWidth());
        assertEquals(64, img.getHeight());
        final File out = new File("target/mspaint-8x10resized.png");
        ImageIO.write(img, "PNG", out);
    }
}