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
package org.apache.nifi.wrapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.wrapping.UnwrapCloakedDagger;
import org.apache.nifi.wrapping.WrapCloakedDagger;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.CheckSum;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.MaskLength;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.TechniqueType;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Set;

/**
 * Unit Tests for Cloaked Dagger Wrapping NiFi Processor.
 */
public class CloakedDaggerProcessorsTest {

    private static final String TEST_GIF_FILENAME = "src/test/resources/inputfile.gif";
    private static final String TEST_JPG_FILENAME = "src/test/resources/inputfile2.jpg";
    private static final String TEST_EXE_FILENAME = "src/test/resources/inputfile3.exe";
    private static final String TEST_WAV_FILENAME = "src/test/resources/inputfile4.wav";
    private static final String TEST_CONTENT = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";
    private static final int MUTLIPLIER = 10000;
    private byte[] testGifFile;
    private byte[] testJpgFile;
    private byte[] testExeFile;
    private byte[] testWavFile;
    private byte[] testBigFile;

    /**
     * Method called before each test.
     *
     * @throws java.lang.Exception
     *             - for any errors
     */
    @Before
    public void setUp() throws Exception {
        File file = new File(TEST_GIF_FILENAME);
        long len = file.length();
        this.testGifFile = new byte[(int) len];
        try (InputStream is = new FileInputStream(file)) {
            int offset = 0;
            int numRead = 0;
            while ((offset < testGifFile.length)
                            && ((numRead = is.read(testGifFile, offset, testGifFile.length - offset)) >= 0)) {
                offset += numRead;
            }
        }

        byte[] singleItem = TEST_CONTENT.getBytes("UTF-8");
        int lenItem = singleItem.length;
        this.testBigFile = new byte[lenItem * MUTLIPLIER];
        for (int i = 0; (i < MUTLIPLIER); i++) {
            System.arraycopy(singleItem, 0, testBigFile, (i * lenItem), lenItem);
        }

        file = new File(TEST_JPG_FILENAME);
        len = file.length();
        this.testJpgFile = new byte[(int) len];
        try (InputStream is = new FileInputStream(file)) {
            int offset = 0;
            int numRead = 0;
            while ((offset < testJpgFile.length)
                            && ((numRead = is.read(testJpgFile, offset, testJpgFile.length - offset)) >= 0)) {
                offset += numRead;
            }
        }

        file = new File(TEST_EXE_FILENAME);
        len = file.length();
        this.testExeFile = new byte[(int) len];
        try (InputStream is = new FileInputStream(file)) {
            int offset = 0;
            int numRead = 0;
            while ((offset < testExeFile.length)
                            && ((numRead = is.read(testExeFile, offset, testExeFile.length - offset)) >= 0)) {
                offset += numRead;
            }
        }

        file = new File(TEST_WAV_FILENAME);
        len = file.length();
        this.testWavFile = new byte[(int) len];
        try (InputStream is = new FileInputStream(file)) {
            int offset = 0;
            int numRead = 0;
            while ((offset < testWavFile.length)
                            && ((numRead = is.read(testWavFile, offset, testWavFile.length - offset)) >= 0)) {
                offset += numRead;
            }
        }
    }

    /**
     * Test for all combinations of input with the following exceptions: MASK_8_BIT, MASK_192_BIT and MASK_256_BIT - all
     * combinations as "Value does not match regular expression: (16|32|64|128)". MASK_16_BIT, MASK_32_BIT &&
     * MASK_64_BIT with Technique2 - as "Mask length is not supported by this technique."
     *
     * @throws IOException upon failure to access test files
     */
    @Test
    public void testWrapUnwrapAllValidSmall() throws IOException {
        for (MaskLength maskLength : MaskLength.values()) {
            if (MaskLength.MASK_8_BIT == maskLength || MaskLength.MASK_192_BIT == maskLength
                            || MaskLength.MASK_256_BIT == maskLength) {
                continue; // Skip 8-Bit, 192-bit and 256-bit as not valid for Processor(s).
            } // End If
            for (TechniqueType useTechnique : TechniqueType.values()) {
                if (MaskLength.MASK_128_BIT != maskLength && TechniqueType.TECHNIQUE_2 == useTechnique) {
                    continue; // Skip everything but 128-Bit / Technique-2 as not supported.
                } // End If
                for (CheckSum headerCheckSum : CheckSum.values()) {
                    for (CheckSum bodyCheckSum : CheckSum.values()) {
                        this.checkWrapThenUnwrap(maskLength, useTechnique, headerCheckSum, bodyCheckSum, testGifFile);
                    } // End For (Body CheckSum)
                } // End For (Header CheckSum)
            } // End For (TechniqueType)
        } // End For (MaskLength)
    }

    /**
     * Test same as testWrapUnwrapAllValidSmall above, but with an exe file
     *
     * @throws IOException upon failure to access test files
     */
    @Test
    public void testWrapUnwrapExe() throws IOException {
        for (MaskLength maskLength : MaskLength.values()) {
            if (MaskLength.MASK_8_BIT == maskLength || MaskLength.MASK_192_BIT == maskLength
                            || MaskLength.MASK_256_BIT == maskLength) {
                continue; // Skip 8-Bit, 192-bit and 256-bit as not valid for Processor(s).
            } // End If
            for (TechniqueType useTechnique : TechniqueType.values()) {
                if (MaskLength.MASK_128_BIT != maskLength && TechniqueType.TECHNIQUE_2 == useTechnique) {
                    continue; // Skip everything but 128-Bit / Technique-2 as not supported.
                } // End If
                for (CheckSum headerCheckSum : CheckSum.values()) {
                    for (CheckSum bodyCheckSum : CheckSum.values()) {
                        this.checkWrapThenUnwrap(maskLength, useTechnique, headerCheckSum, bodyCheckSum, testExeFile);
                    } // End For (Body CheckSum)
                } // End For (Header CheckSum)
            } // End For (TechniqueType)
        } // End For (MaskLength)
    }

    /**
     * Test same as testWrapUnwrapAllValidSmall above, but with an exe file
     *
     * @throws IOException upon failure to access test files
     */
    @Test
    public void testWrapUnwrapWav() throws IOException {
        for (MaskLength maskLength : MaskLength.values()) {
            if (MaskLength.MASK_8_BIT == maskLength || MaskLength.MASK_192_BIT == maskLength
                            || MaskLength.MASK_256_BIT == maskLength) {
                continue; // Skip 8-Bit, 192-bit and 256-bit as not valid for Processor(s).
            } // End If
            for (TechniqueType useTechnique : TechniqueType.values()) {
                if (MaskLength.MASK_128_BIT != maskLength && TechniqueType.TECHNIQUE_2 == useTechnique) {
                    continue; // Skip everything but 128-Bit / Technique-2 as not supported.
                } // End If
                for (CheckSum headerCheckSum : CheckSum.values()) {
                    for (CheckSum bodyCheckSum : CheckSum.values()) {
                        this.checkWrapThenUnwrap(maskLength, useTechnique, headerCheckSum, bodyCheckSum, testWavFile);
                    } // End For (Body CheckSum)
                } // End For (Header CheckSum)
            } // End For (TechniqueType)
        } // End For (MaskLength)
    }

    /**
     * Test same as testWrapUnwrapAllValidSmall above, but with a larger jpg
     *
     * @throws IOException upon failure to access test files
     */
    @Test
    public void testWrapUnwrapAllValidLarger() throws IOException {
        for (MaskLength maskLength : MaskLength.values()) {
            if (MaskLength.MASK_8_BIT == maskLength || MaskLength.MASK_192_BIT == maskLength
                            || MaskLength.MASK_256_BIT == maskLength) {
                continue; // Skip 8-Bit, 192-bit and 256-bit as not valid for Processor(s).
            } // End If
            for (TechniqueType useTechnique : TechniqueType.values()) {
                if (MaskLength.MASK_128_BIT != maskLength && TechniqueType.TECHNIQUE_2 == useTechnique) {
                    continue; // Skip everything but 128-Bit / Technique-2 as not supported.
                } // End If
                for (CheckSum headerCheckSum : CheckSum.values()) {
                    for (CheckSum bodyCheckSum : CheckSum.values()) {
                        this.checkWrapThenUnwrap(maskLength, useTechnique, headerCheckSum, bodyCheckSum, testJpgFile);
                    } // End For (Body CheckSum)
                } // End For (Header CheckSum)
            } // End For (TechniqueType)
        } // End For (MaskLength)
    }

    @Test
    public void testWrapUnwrapMask128Tech1HdrNoneBdyNone() throws IOException {
        this.checkWrapThenUnwrap(MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_1, CheckSum.NO_CHECKSUM,
                        CheckSum.NO_CHECKSUM, testGifFile);
    }

    @Test
    public void testWrapUnwrapMask128Tech1HdrCrcBdyCrc() throws IOException {
        this.checkWrapThenUnwrap(MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_1, CheckSum.CRC_32_CHECKSUM,
                        CheckSum.CRC_32_CHECKSUM, testGifFile);
    }

    @Test
    public void testWrapUnwrapMask128Tech2HdrCrcBdyCrc() throws IOException {
        this.checkWrapThenUnwrap(MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_2, CheckSum.CRC_32_CHECKSUM,
                        CheckSum.CRC_32_CHECKSUM, testGifFile);
    }

    @Test
    public void testWrapUnwrapMask128Tech1HdrCrcBdyCrcBig() throws IOException {
        this.checkWrapThenUnwrap(MaskLength.MASK_128_BIT, TechniqueType.TECHNIQUE_1, CheckSum.CRC_32_CHECKSUM,
                        CheckSum.CRC_32_CHECKSUM, testBigFile);
    }

    @Test
    public void testWrappingProcessorProperties() throws IOException {
        TestRunner wrapper = this.setupWrapper(MaskLength.MASK_16_BIT, TechniqueType.TECHNIQUE_1,
                        CheckSum.CRC_32_CHECKSUM, CheckSum.CRC_32_CHECKSUM, testGifFile);
        wrapper.assertValid();

        // Test all combinations with an invalid technique type
        wrapper.setProperty(WrapCloakedDagger.TECH_TYPE_PROP, "InvalidTechnique");
        for (MaskLength maskLength : MaskLength.values()) {
            wrapper.setProperty(WrapCloakedDagger.MASK_LENGTH_PROP, Integer.toString(maskLength.intValue()));
            for (CheckSum headerCheckSum : CheckSum.values()) {
                if (CheckSum.NO_CHECKSUM == headerCheckSum) {
                    try {
                        wrapper.setProperty(WrapCloakedDagger.HEADER_CS_PROP, headerCheckSum.stringValue());
                    } catch (NullPointerException npe) {
                        continue;
                    }
                }
                wrapper.setProperty(WrapCloakedDagger.HEADER_CS_PROP, headerCheckSum.stringValue());
                for (CheckSum bodyCheckSum : CheckSum.values()) {
                    if (CheckSum.NO_CHECKSUM == bodyCheckSum) {
                        try {
                            wrapper.setProperty(WrapCloakedDagger.BODY_CS_PROP, bodyCheckSum.stringValue());
                        } catch (NullPointerException npe) {
                            continue;
                        }
                    }
                    wrapper.setProperty(WrapCloakedDagger.BODY_CS_PROP, bodyCheckSum.stringValue());
                    wrapper.assertNotValid();
                } // End For (Body CheckSum)
            } // End For (Header CheckSum)
        } // End For (MaskLength maskLength)

        // Test all combinations with an invalid headerCheckSum
        wrapper.setProperty(WrapCloakedDagger.HEADER_CS_PROP, "InvalidChecksum");
        for (MaskLength maskLength : MaskLength.values()) {
            wrapper.setProperty(WrapCloakedDagger.MASK_LENGTH_PROP, Integer.toString(maskLength.intValue()));
            for (TechniqueType useTechnique : TechniqueType.values()) {
                wrapper.setProperty(WrapCloakedDagger.TECH_TYPE_PROP, useTechnique.stringValue());
                for (CheckSum bodyCheckSum : CheckSum.values()) {
                    if (CheckSum.NO_CHECKSUM == bodyCheckSum) {
                        try {
                            wrapper.setProperty(WrapCloakedDagger.BODY_CS_PROP, bodyCheckSum.stringValue());
                        } catch (NullPointerException npe) {
                            continue;
                        }
                    }
                    wrapper.setProperty(WrapCloakedDagger.BODY_CS_PROP, bodyCheckSum.stringValue());
                    wrapper.assertNotValid();
                } // End For (Body CheckSum)
            } // End For (TechniqueType)
        } // End For (MaskLength maskLength)

        // Test all combinations with an invalid bodyCheckSum
        wrapper.setProperty(WrapCloakedDagger.BODY_CS_PROP, "InvalidChecksum");
        for (MaskLength maskLength : MaskLength.values()) {
            wrapper.setProperty(WrapCloakedDagger.MASK_LENGTH_PROP, Integer.toString(maskLength.intValue()));
            for (TechniqueType useTechnique : TechniqueType.values()) {
                wrapper.setProperty(WrapCloakedDagger.TECH_TYPE_PROP, useTechnique.stringValue());
                for (CheckSum headerCheckSum : CheckSum.values()) {
                    if (CheckSum.NO_CHECKSUM == headerCheckSum) {
                        try {
                            wrapper.setProperty(WrapCloakedDagger.HEADER_CS_PROP, headerCheckSum.stringValue());
                        } catch (NullPointerException npe) {
                            continue;
                        }
                    }
                    wrapper.setProperty(WrapCloakedDagger.HEADER_CS_PROP, headerCheckSum.stringValue());
                    wrapper.assertNotValid();
                } // End For (Header CheckSum)
            } // End For (TechniqueType)
        } // End For (MaskLength maskLength)

        // Test the MaskLength options
        wrapper.setProperty(WrapCloakedDagger.HEADER_CS_PROP, CheckSum.CRC_32_CHECKSUM.stringValue());
        wrapper.setProperty(WrapCloakedDagger.BODY_CS_PROP, CheckSum.CRC_32_CHECKSUM.stringValue());
        for (MaskLength maskLength : MaskLength.values()) {
            for (TechniqueType useTechnique : TechniqueType.values()) {
                wrapper.setProperty(WrapCloakedDagger.MASK_LENGTH_PROP, Integer.toString(maskLength.intValue()));
                wrapper.setProperty(WrapCloakedDagger.TECH_TYPE_PROP, useTechnique.stringValue());
                if (MaskLength.MASK_8_BIT == maskLength || MaskLength.MASK_192_BIT == maskLength
                                || MaskLength.MASK_256_BIT == maskLength) {
                    wrapper.assertNotValid();
                } else if (TechniqueType.TECHNIQUE_2 == useTechnique && MaskLength.MASK_128_BIT != maskLength) {
                    // Technique 2 only supports Mask Length 128.
                    wrapper.assertNotValid();
                } else {
                    wrapper.assertValid();
                }
            } // End For (TechniqueType)
        } // End For (MaskLength maskLength)
    }

    @Test
    public void testWrapGetRelationships() throws Exception {
        final TestRunner wrapper = TestRunners.newTestRunner(new WrapCloakedDagger());
        Set<Relationship> testResult = wrapper.getProcessor().getRelationships();
        assertNotNull(testResult);
        assertTrue("Contains some relationships", testResult.size() > 0);
    }

    @Test
    public void testUnWrapGetRelationships() throws Exception {
        final TestRunner wrapper = TestRunners.newTestRunner(new UnwrapCloakedDagger());
        Set<Relationship> testResult = wrapper.getProcessor().getRelationships();
        assertNotNull(testResult);
        assertTrue("Contains some relationships", testResult.size() > 0);
    }

    @Test
    public void testWrapOnTriggerNullFlowFileForCoverage() throws Exception {
        final TestRunner wrapper = TestRunners.newTestRunner(new WrapCloakedDagger());

        wrapper.getProcessor().onTrigger(wrapper.getProcessContext(), wrapper.getProcessSessionFactory());
    }

    @Test
    public void testUnWrapOnTriggerNullFlowFileForCoverage() throws Exception {
        final TestRunner wrapper = TestRunners.newTestRunner(new UnwrapCloakedDagger());

        wrapper.getProcessor().onTrigger(wrapper.getProcessContext(), wrapper.getProcessSessionFactory());
    }

    private void checkWrapThenUnwrap(final MaskLength maskLength, final TechniqueType useTechnique,
        final CheckSum headerCheckSum, final CheckSum bodyCheckSum, final byte[] originalData) throws IOException {
        final TestRunner wrapper = this.setupWrapper(maskLength, useTechnique, headerCheckSum, bodyCheckSum,
                        originalData);
        wrapper.run();
        wrapper.assertTransferCount(WrapCloakedDagger.SUCCESS_RELATIONSHIP, 1);
        wrapper.assertTransferCount(WrapCloakedDagger.ERROR_RELATIONSHIP, 0);

        final MockFlowFile wrapperOut = wrapper.getFlowFilesForRelationship(WrapCloakedDagger.SUCCESS_RELATIONSHIP)
                        .get(0);
        assertTrue(wrapperOut.getSize() > 0);

        final TestRunner unwrapper = this.setupUnwrapper(wrapperOut.toByteArray());
        unwrapper.run();
        unwrapper.assertTransferCount(UnwrapCloakedDagger.SUCCESS_RELATIONSHIP, 1);
        unwrapper.assertTransferCount(UnwrapCloakedDagger.ERROR_RELATIONSHIP, 0);

        final MockFlowFile unwrapperOut = unwrapper.getFlowFilesForRelationship(
                        UnwrapCloakedDagger.SUCCESS_RELATIONSHIP).get(0);
        assertTrue(unwrapperOut.getSize() > 0);

        // Check unwrapped Flowfile is different to input.
        assertFalse(wrapperOut.compareTo(unwrapperOut) == 0);

        // Check unwrapped Flowfile is same as original.
        assertEquals("Unwrapped Size Equals Expected", (long) originalData.length, unwrapperOut.getSize());
        assertTrue("Bytpe Array Match", Arrays.equals(originalData, unwrapperOut.toByteArray()));
    }

    private TestRunner setupWrapper(final MaskLength maskLength, final TechniqueType useTechnique,
        final CheckSum headerCheckSum, final CheckSum bodyCheckSum, final byte[] enqueueData) throws IOException {
        final TestRunner wrapper = TestRunners.newTestRunner(new WrapCloakedDagger());
        wrapper.setProperty(WrapCloakedDagger.MASK_LENGTH_PROP, Integer.toString(maskLength.intValue()));
        wrapper.setProperty(WrapCloakedDagger.TECH_TYPE_PROP, useTechnique.stringValue());
        if (CheckSum.NO_CHECKSUM == headerCheckSum) {
            wrapper.setProperty(WrapCloakedDagger.HEADER_CS_PROP, "NONE");
        } else {
            wrapper.setProperty(WrapCloakedDagger.HEADER_CS_PROP, headerCheckSum.stringValue());
        } // End If
        if (CheckSum.NO_CHECKSUM == bodyCheckSum) {
            wrapper.setProperty(WrapCloakedDagger.BODY_CS_PROP, "NONE");
        } else {
            wrapper.setProperty(WrapCloakedDagger.BODY_CS_PROP, bodyCheckSum.stringValue());
        } // End If
        wrapper.enqueue(enqueueData);

        return wrapper;
    }

    private TestRunner setupUnwrapper(final byte[] enqueueData) throws IOException {
        final TestRunner unwrapper = TestRunners.newTestRunner(new UnwrapCloakedDagger());
        unwrapper.enqueue(enqueueData);
        return unwrapper;
    }
}
