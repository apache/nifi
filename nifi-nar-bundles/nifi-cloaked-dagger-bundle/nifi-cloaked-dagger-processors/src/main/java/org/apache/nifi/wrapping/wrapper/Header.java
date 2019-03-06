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

//CHECKSTYLE.OFF: CustomImportOrderCheck
import org.apache.commons.lang3.ArrayUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

//CHECKSTYLE.ON: CustomImportOrderCheck

/**
 * Header information.
 */
public class Header {
    private static final int MAGIC_1 = 0xD1DF5FFF;
    private static final int MAGIC_2 = 0xFF5FDFD1;

    private static final short MAJOR_VERSION = 1;
    private static final short MINOR_VERSION = 0;

    private final Technique encap;
    private final Technique headerCheck;
    private final Technique bodyCheck;
    private final String mode;

    /**
     * Construct a header using the supplied parameters.
     *
     * @param encapsulation
     *            - technique to use.
     * @param headerChecksum
     *            - the checksum for the header.
     * @param bodyChecksum
     *            - the checksum for the body.
     */
    public Header(final Technique encapsulation, final String headerChecksum, final String bodyChecksum) {
        this.encap = encapsulation;

        // Check that a header checksum has been specified.
        if ("CRC32".equals(headerChecksum)) {
            mode = "CRC32";
            // Create the technique with no data.
            headerCheck = new Technique(Technique.TECHNIQUE_1, new byte[] {});
        } else if ("SHA256".equals(headerChecksum)) {
            mode = "SHA256";
            // Create the technique with no data.
            headerCheck = new Technique(Technique.TECHNIQUE_2, new byte[] {});
        } else {
            mode = "";
            headerCheck = null;
        }

        // Check that a body checksum has been specified.
        if ("CRC32".equals(bodyChecksum)) {
            // Create the technique with no data.
            bodyCheck = new Technique(Technique.TECHNIQUE_1, new byte[] {});
        } else if ("SHA256".equals(bodyChecksum)) {
            // Create the technique with no data.
            bodyCheck = new Technique(Technique.TECHNIQUE_2, new byte[] {});
        } else {
            bodyCheck = null;
        }
    }

    /**
     * Method to return the header as a byte array.
     *
     * @return the header as a byte array.
     */
    public byte[] getHeaderBytes() {
        // Use this to assemble the header bytes.
        byte[] result = null;

        // Add the first 4 byte magic number to the result.
        result = ArrayUtils.addAll(result, intToBytes(MAGIC_1));

        // Add the 4 byte version (two 2 byte components, major and minor).
        result = ArrayUtils.addAll(result, shortToBytes(MAJOR_VERSION));
        result = ArrayUtils.addAll(result, shortToBytes(MINOR_VERSION));

        // If an encapsulation is specified, add them in, or add the empty 8 bytes required.
        if (encap == null) {
            result = ArrayUtils.addAll(result, new byte[8]);
        } else {
            result = ArrayUtils.addAll(result, encap.getTechniqueBytes());
        }

        // If a header checksum is specified, add it, or add the empty 8 bytes required.
        if (headerCheck == null) {
            result = ArrayUtils.addAll(result, new byte[8]);
        } else {
            result = ArrayUtils.addAll(result, headerCheck.getTechniqueBytes());
        }

        // If a body checksum is specified, add it, or add the empty 8 bytes required.
        if (bodyCheck == null) {
            result = ArrayUtils.addAll(result, new byte[8]);
        } else {
            result = ArrayUtils.addAll(result, bodyCheck.getTechniqueBytes());
        }
        final byte[] headerLength;

        // Header length. Add 8 to account for the header length + magic number we'll add in next. Also
        // add the length of the checksum if its there.
        if ("CRC32".equals(mode)) {
            // Current length +4 for magic number,+4 for length, and + the technique length.
            // CRC32 length = 4 bytes.
            headerLength = intToBytes(result.length + 8 + 4);
        } else if ("SHA256".equals(mode)) {
            // Current length +4 for magic number,+4 for length, and + the technique length.
            // SHA256 length = 32 bytes.
            headerLength = intToBytes(result.length + 8 + 32);
        } else {
            // If no header check sum is specified, we take the current length + 4 bytes for the the
            // length
            // we're about to add, and another 4 for the magic number at the end.
            headerLength = intToBytes(result.length + 8);
        }

        // Now we know the length, populate the header length at index 8.
        result = ArrayUtils.add(result, 8, headerLength[0]);
        result = ArrayUtils.add(result, 9, headerLength[1]);
        result = ArrayUtils.add(result, 10, headerLength[2]);
        result = ArrayUtils.add(result, 11, headerLength[3]);

        // If a header checksum is specified, it must be added here.
        if (headerCheck != null) {
            // Get the header checksum for the header so far.
            result = ArrayUtils.addAll(result, calculateChecksum(result));
        }

        // Add the final magic number.
        result = ArrayUtils.addAll(result, intToBytes(MAGIC_2));

        return result;
    }

    /**
     * Calculates the checksum for the header.
     *
     * @param result
     *            - byte array to use.
     * @return the calculated checksum for the header.
     */
    private byte[] calculateChecksum(byte[] result) {
        byte[] checksumResult = null;

        // This won't be called without a headerCheck being populated, so no null check required.
        if ("CRC32".equals(mode)) {
            checksumResult = new byte[4];
            final Checksum checksum = new CRC32();
            checksum.update(result, 0, result.length);
            byte[] tempResult = ByteBuffer.allocate(8).putLong(checksum.getValue()).array();

            // Chop off the 4 empty bytes at the start of the checksum.
            System.arraycopy(tempResult, 4, checksumResult, 0, 4);

        } else if ("SHA256".equals(mode)) {
            try {
                final MessageDigest md;
                md = MessageDigest.getInstance("SHA-256");
                md.update(result, 0, result.length);
                checksumResult = md.digest();
            } catch (NoSuchAlgorithmException e) {
                // This should never happen as the algorithm is encaphard coded.
                throw new RuntimeException("Fatal error occurred while setting up checksum mechanism: " + e);
            }
        }
        return checksumResult;
    }

    /**
     * Method to convert an int to a byte array.
     *
     * @param value
     *            - int to be converted.
     * @return a byte array created from the given int value.
     */
    private byte[] intToBytes(int value) {
        // Give the byte buffer a capacity of 4 bytes.
        final ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(value);
        // Need network byte order.
        buffer.order(ByteOrder.BIG_ENDIAN);
        return buffer.array();
    }

    /**
     * Method to convert a short to a byte array.
     *
     * @param value
     *            - short to be converted.
     * @return a byte array created from the given short value.
     */
    private byte[] shortToBytes(short value) {
        // Give the byte buffer a capacity of 2 bytes.
        final ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort(value);
        // Need network byte order.
        buffer.order(ByteOrder.BIG_ENDIAN);
        return buffer.array();
    }
}
