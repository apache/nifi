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
package org.apache.nifi.processors.ocr;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;


public class TesseractOCRProcessorTest {

    private TestRunner testRunner;

    private static final String INVALID_TESS_DATA_DIR = "/invalid/path/to/no/tessdata";
    private static final String VALID_TESS_DATA_DIR = "src/test/resources/tessdata";
    private static final String QUICK_BROWN_FOX_FILE = "src/test/resources/images/QuickBrownFox.jpg";
    private static final String QUICK_BROWN_FOX_VALID = "The quick brown fox\njumped over the 5\nlazy dogs!\n\n";

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(TesseractOCRProcessor.class);
        testRunner.setProperty(TesseractOCRProcessor.TESS_DATA_PATH, VALID_TESS_DATA_DIR);
        testRunner.setProperty(TesseractOCRProcessor.TESSERACT_LANGUAGE,TesseractOCRProcessor.SUPPORTED_LANGUAGES.iterator().next());
    }

    @Test(expected = AssertionError.class)
    public void testNonExistentTesseractInstallation() throws Exception {
        testRunner.setProperty(TesseractOCRProcessor.TESS_DATA_PATH, INVALID_TESS_DATA_DIR);

        testRunner.enqueue(new File(QUICK_BROWN_FOX_FILE).toPath());
        testRunner.run();
    }

    @Test(expected = AssertionError.class)
    public void testNonExistantLanguage() throws Exception {
        testRunner.setProperty(TesseractOCRProcessor.TESSERACT_LANGUAGE, "zzz");

        testRunner.enqueue(new File(QUICK_BROWN_FOX_FILE).toPath());
        testRunner.run();
    }

    @Test
    public void testUnsupportedImageFormat() throws IOException {

        testRunner.enqueue(new File("src/test/resources/images/invalidInput.json").toPath());
        testRunner.run();

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(TesseractOCRProcessor.REL_SUCCESS);
        testRunner.assertTransferCount(TesseractOCRProcessor.REL_SUCCESS, 0);
        testRunner.assertTransferCount(TesseractOCRProcessor.REL_UNSUPPORTED_IMAGE_FORMAT, 1);
        testRunner.assertTransferCount(TesseractOCRProcessor.REL_FAILURE, 0);
        testRunner.assertTransferCount(TesseractOCRProcessor.REL_ORIGINAL, 1);
    }

    @Test
    public void testQuickBrownFoxOutput() throws Exception {
        testRunner.enqueue(new File(QUICK_BROWN_FOX_FILE).toPath());
        testRunner.run();

        List<MockFlowFile> ffs = testRunner.getFlowFilesForRelationship(TesseractOCRProcessor.REL_SUCCESS);
        ffs.get(0).assertContentEquals(QUICK_BROWN_FOX_VALID);

        testRunner.assertTransferCount(TesseractOCRProcessor.REL_SUCCESS, 1);
        testRunner.assertTransferCount(TesseractOCRProcessor.REL_UNSUPPORTED_IMAGE_FORMAT, 0);
        testRunner.assertTransferCount(TesseractOCRProcessor.REL_FAILURE, 0);
        testRunner.assertTransferCount(TesseractOCRProcessor.REL_ORIGINAL, 1);
    }

}
