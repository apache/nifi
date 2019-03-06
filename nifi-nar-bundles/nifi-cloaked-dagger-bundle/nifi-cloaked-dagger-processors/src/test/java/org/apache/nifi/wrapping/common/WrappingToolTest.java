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
package org.apache.nifi.wrapping.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.nifi.wrapping.common.WrappingTool;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.CheckSum;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.MaskLength;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.TechniqueType;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Unit Tests for WrappingTool.
 */
public class WrappingToolTest {

    private static String TEST_GIF_FILENAME = "src/test/resources/inputfile.gif";
    private static String OUTPUT_WRAPPED_FILE = "target/wrapped.local";
    private static String OUTPUT_UNWRAPPED_FILE = "target/unwrapped.local";

    /**
     * Test for default constructor.
     */
    @SuppressWarnings("unused")
    private static final WrappingTool tool = new WrappingTool();

    /**
     * Test for all combinations of input with the following exceptions: TECHNIQUE_2 with mask lengths of MASK_8_BIT,
     * MASK_16_BIT, MASK_32_BIT and MASK_64_BIT
     *
     * @throws IOException upon failure to access test files
     */
    @Test
    public void testWrapUnwrapAllValidSmall() throws IOException {
        File fileIn = new File(TEST_GIF_FILENAME);
        File fileWrapOut = new File(OUTPUT_WRAPPED_FILE);
        File fileUnwrapOut = new File(OUTPUT_UNWRAPPED_FILE);
        String[] args = new String[7];

        for (MaskLength maskLength : MaskLength.values()) {
            args[4] = Integer.toString(maskLength.intValue());
            for (TechniqueType useTechnique : TechniqueType.values()) {
                args[3] = useTechnique.stringValue();
                if ((MaskLength.MASK_8_BIT == maskLength || MaskLength.MASK_16_BIT == maskLength
                                || MaskLength.MASK_32_BIT == maskLength || MaskLength.MASK_64_BIT == maskLength)
                                && TechniqueType.TECHNIQUE_2 == useTechnique) {
                    continue;
                }
                for (CheckSum headerCheckSum : CheckSum.values()) {
                    args[5] = headerCheckSum.stringValue();
                    for (CheckSum bodyCheckSum : CheckSum.values()) {
                        args[6] = bodyCheckSum.stringValue();

                        args[0] = fileIn.getPath();
                        args[1] = fileWrapOut.getPath();
                        args[2] = "wrap";
                        WrappingTool.main(args);
                        assertNotEquals(fileIn.length(), fileWrapOut.length());

                        args[0] = fileWrapOut.getPath();
                        args[1] = fileUnwrapOut.getPath();
                        args[2] = "unwrap";
                        WrappingTool.main(args);
                        assertEquals(fileIn.length(), fileUnwrapOut.length());
                    } // End For (Body CheckSum)
                } // End For (Header CheckSum)
            } // End For (TechniqueType)
        } // End For (MaskLength)
    }

    /**
     * Test for mask length of null
     *
     * @throws IOException upon failure to access test files
     */
    @Test(expected = NullPointerException.class)
    public void testWrapMaskLengthNull() throws IOException {
        WrappingTool.wrap(TEST_GIF_FILENAME, OUTPUT_WRAPPED_FILE, "Technique1", "5",
                        "CRC32", "CRC32");
    }

    /**
     * Test for wrong technique type
     *
     * @throws IOException upon failure to access test files
     */
    @Test(expected = NullPointerException.class)
    public void testWrapInvalidTechnique() throws IOException {
        WrappingTool.wrap(TEST_GIF_FILENAME, OUTPUT_WRAPPED_FILE, "Technique8", "16",
                        "CRC32", "CRC32");
    }

    /**
     * Test for large file
     *
     * @throws IOException upon failure to access test files
     */
    @Test
    public void testWrapLargeFile() throws IOException {
        WrappingTool.wrap(TEST_GIF_FILENAME, OUTPUT_WRAPPED_FILE, "Technique1",
                        "16", "CRC32", "CRC32");
    }

    /**
     * Test for small file
     *
     * @throws IOException  upon failure to access test files
     */
    @Test
    public void testProblemFile497Bytes() throws IOException {
        WrappingTool.wrap("src/test/resources/test-file-497", "target/test-file-497.wrap.local",
                        "Technique2", "128", "SHA256", "SHA256");
        WrappingTool.unwrap("target/test-file-497.wrap.local",
                        "target/test-file-497.unwrap.local");
    }

    /**
     * Test for small file
     *
     * @throws IOException  upon failure to access test files
     */
    @Test
    public void testNoProblemFile496Bytes() throws IOException {
        WrappingTool.wrap("src/test/resources/test-file-496", "target/test-file-496.wrap.local",
                        "Technique2", "128", "SHA256", "SHA256");
        WrappingTool.unwrap("target/test-file-496.wrap.local",
                        "target/test-file-496.unwrap.local");
    }

}