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
package org.apache.nifi.wrapping.wrapper;

import org.apache.commons.lang3.ArrayUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

/**
 * Provides access to wrapping classes.
 */
public class WrapperFactory {
    /**
     * Specifies CheckSum values to use when requesting a wrapper.
     */
    public enum CheckSum {
        NO_CHECKSUM(null), CRC_32_CHECKSUM("CRC32"), SHA_256_CHECKSUM("SHA256");

        // String value.
        private final String value;

        // Constructor
        CheckSum(String value) {
            this.value = value;
        }

        public String stringValue() {
            return value;
        }
    }

    /**
     * Specifies Technique values to use when requesting a wrapper.
     */
    public enum TechniqueType {
        TECHNIQUE_1("Technique1"), TECHNIQUE_2("Technique2");

        // String value.
        private final String technique;

        // Constructor
        TechniqueType(String technique) {
            this.technique = technique;
        }

        public String stringValue() {
            return technique;
        }
    }

    /**
     * Specifies mask lengths to use when creating a Wrapper.
     */
    public enum MaskLength {
        MASK_8_BIT(8),
        MASK_16_BIT(16),
        MASK_32_BIT(32),
        MASK_64_BIT(64),
        MASK_128_BIT(128),
        MASK_192_BIT(192),
        MASK_256_BIT(256);

        // Length of mask.
        private final int maskLength;

        // Constructor
        MaskLength(int maskLength) {
            this.maskLength = maskLength;
        }

        // Get the int value.
        public int intValue() {
            return maskLength;
        }
    }

    /**
     * Returns a default wrapper to use for encapsulating data. This is an AES (technique 2) wrapper with no checksums.
     * Default mask size is 16.
     *
     * @return the wrapper object.
     * @throws IOException
     *             If the wrapper cannot write to the output stream specified.
     */
    public Wrapper getWrapper(OutputStream os) throws IOException {
        // Technique wrapper treats null as "no checksum required".
        return new Technique2Wrapper(os, getMask(MaskLength.MASK_128_BIT), null, null);
    }

    /**
     * Returns a wrapper of the specified type to use for encapsulating data. CRC-32 checksums are used for both header
     * and body. Default mask size is 16 bytes.
     *
     * @param os
     *            Output stream to use.
     * @param type
     *            Use static strings in this class to define technique.
     * @return Wrapper to use.
     */
    public Wrapper getWrapper(OutputStream os, TechniqueType type) throws IOException {
        if (TechniqueType.TECHNIQUE_1.equals(type)) {
            return new Technique1Wrapper(os, getMask(MaskLength.MASK_128_BIT),
                            CheckSum.CRC_32_CHECKSUM.stringValue(), CheckSum.CRC_32_CHECKSUM.stringValue());
        } else {
            return new Technique2Wrapper(os, getMask(MaskLength.MASK_128_BIT),
                            CheckSum.CRC_32_CHECKSUM.stringValue(), CheckSum.CRC_32_CHECKSUM.stringValue());
        }
    }

    /**
     * Returns a wrapper of the specified type to use for encapsulating data. Checksums should be specified using the
     * static Strings provided with this class.
     *
     * @param os
     *            OutputStream to write to.
     *
     * @param maskLength
     *            Mask Length to use (use provided int in this class).
     *
     * @param type
     *            Technique type to use (use provided String in this class).
     * @param headerChecksum
     *            Checksum type to use (use provided String in this class).
     * @param bodyChecksum
     *            Checksum type to use (use provided String in this class).
     * @return the wrapper object.
     * @throws IOException
     *             - for any problems getting the wrapper.
     */
    public Wrapper getWrapper(OutputStream os, MaskLength maskLength, TechniqueType type, CheckSum headerChecksum,
        CheckSum bodyChecksum) throws IOException {
        if (TechniqueType.TECHNIQUE_1.equals(type)) {
            return new Technique1Wrapper(os, getMask(maskLength), headerChecksum.value, bodyChecksum.value);
        } else if (TechniqueType.TECHNIQUE_2.equals(type)) {
            if (maskLength.intValue() < 128 && maskLength.intValue() != 32) {
                throw new RuntimeException("Mask length of " + maskLength + " is not supported by this technique.");
            }
            return new Technique2Wrapper(os, getMask(maskLength), headerChecksum.stringValue(),
                            bodyChecksum.stringValue());
        } else {
            return null;
        }
    }

    /**
     * Returns a Technique 2 wrapper to use for encapsulating data. Checksums must be specified using one of the static
     * Strings included within this class.
     *
     * @param os
     *            OutputStream to write to.
     * @param maskConfig
     *            Config file containing the list of masks in use.
     * @param maskSelector
     *            int value specifying the line number of the mask to use.
     * @param headerChecksum
     *            (use provided String from this class).
     * @param bodyChecksum
     *            (use provided String from this class).
     * @return the wrapper object.
     * @throws IOException
     *             - for any problems getting the wrapper.
     */
    public Wrapper getWrapper(OutputStream os, File maskConfig, int maskSelector, CheckSum headerChecksum,
        CheckSum bodyChecksum) throws IOException {
        return new Technique2Wrapper(os, maskConfig, maskSelector, headerChecksum.stringValue(),
                        bodyChecksum.stringValue());
    }

    /**
     * Returns a pseudo-randomly generated mask of length maskLength.
     *
     * @param maskLength
     *            The Mask Length to use.
     * @return the mask as a byte array.
     */
    private byte[] getMask(MaskLength maskLength) {
        final Random random = new Random();

        // Convert from bits to bytes.
        final int maskLengthInt = maskLength.intValue() / 8;

        byte[] randomBytes = new byte[] {};

        for (int i = 0; i < maskLengthInt; i++) {
            // Add 1 to ensure we don't include 0 in the generated mask.
            randomBytes = ArrayUtils.add(randomBytes, (byte) (random.nextInt() + 1));
        }
        return randomBytes;
    }
}
