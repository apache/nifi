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

//Dev use only.

package org.apache.nifi.wrapping.wrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.nifi.wrapping.wrapper.Wrapper;
import org.apache.nifi.wrapping.wrapper.WrapperFactory;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.CheckSum;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.MaskLength;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.TechniqueType;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class WrapperFactoryTest {

    private static String OUTPUT_TEMP_FILE = "target/tmp.local";

    private static final WrapperFactory wrapperFactory = new WrapperFactory();

    /**
     * Test the WrapperFactory getWrapper(OutputStream os) method
     *
     * @throws IOException upon failure to access test files
     */
    @Test
    public void getWrapperOutputStreamTest() throws IOException {
        File fileOut = new File(OUTPUT_TEMP_FILE);
        try (BufferedOutputStream bufferedOut = new BufferedOutputStream(new FileOutputStream(fileOut))) {
            Wrapper wrapper = wrapperFactory.getWrapper(bufferedOut);
            assertNotNull(wrapper);
            wrapper.close();
            assertNotEquals(0L, fileOut.length());
        }
    }

    /**
     * Test the WrapperFactory getWrapper(OutputStream os, TechniqueType type) method
     *
     * @throws IOException upon failure to access test files
     */
    @Test
    public void getWrapperOsTechniqueTest() throws IOException {
        File fileOut = new File(OUTPUT_TEMP_FILE);
        try (BufferedOutputStream bufferedOut = new BufferedOutputStream(new FileOutputStream(fileOut))) {
            Wrapper wrapper1 = wrapperFactory.getWrapper(bufferedOut, TechniqueType.TECHNIQUE_1);
            assertNotNull(wrapper1);
            wrapper1.close();
            assertNotEquals(0L, fileOut.length());
        }
        try (BufferedOutputStream bufferedOut = new BufferedOutputStream(new FileOutputStream(fileOut))) {
            Wrapper wrapper2 = wrapperFactory.getWrapper(bufferedOut, TechniqueType.TECHNIQUE_2);
            assertNotNull(wrapper2);
            wrapper2.close();
            assertNotEquals(0L, fileOut.length());
        }
    }

    /**
     * Test the WrapperFactory getWrapper(OutputStream os, MaskLength maskLength, TechniqueType type, CheckSum
     * headerChecksum, CheckSum bodyChecksum) method
     *
     * @throws IOException upon failure to access test files
     */
    @Test
    public void getWrapperOsMaskLengthTest() throws IOException {
        File fileOut = new File(OUTPUT_TEMP_FILE);
        try (BufferedOutputStream bufferedOut = new BufferedOutputStream(new FileOutputStream(fileOut))) {
            Wrapper wrapper1 = wrapperFactory.getWrapper(bufferedOut, MaskLength.MASK_64_BIT,
                            TechniqueType.TECHNIQUE_1, CheckSum.CRC_32_CHECKSUM, CheckSum.CRC_32_CHECKSUM);
            assertNotNull(wrapper1);
            wrapper1.close();
            assertNotEquals(0L, fileOut.length());
        }

        try (BufferedOutputStream bufferedOut = new BufferedOutputStream(new FileOutputStream(fileOut))) {
            Wrapper wrapper2 = wrapperFactory.getWrapper(bufferedOut, MaskLength.MASK_128_BIT,
                            TechniqueType.TECHNIQUE_2, CheckSum.CRC_32_CHECKSUM, CheckSum.CRC_32_CHECKSUM);
            assertNotNull(wrapper2);
            wrapper2.close();
            assertNotEquals(0L, fileOut.length());
        }

        try (BufferedOutputStream bufferedOut = new BufferedOutputStream(new FileOutputStream(fileOut))) {
            try {
                @SuppressWarnings("unused")
                Wrapper wrapper2 = wrapperFactory.getWrapper(bufferedOut, MaskLength.MASK_64_BIT,
                                TechniqueType.TECHNIQUE_2, CheckSum.CRC_32_CHECKSUM, CheckSum.CRC_32_CHECKSUM);
                fail("Exception not thrown as expected.");
            } catch (RuntimeException re) {
                assertEquals("Mask length of MASK_64_BIT is not supported by this technique.", re.getMessage());
            }
        }
    }
}
