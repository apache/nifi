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

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;

import org.apache.nifi.wrapping.common.CRC32Helper;
import org.apache.nifi.wrapping.common.CheckSumHelper;
import org.apache.nifi.wrapping.common.SHA256Helper;

//CHECKSTYLE.ON: CustomImportOrderCheck

/**
 * Wraps a stream of data using technique 1.
 */
public class Technique1Wrapper extends FilterOutputStream implements Wrapper {
    /**
     * Specify SHA256 checksum use.
     */
    public static final String SHA_256 = "SHA256";

    /**
     * Specify CRC32 checksum use.
     */
    public static final String CRC_32 = "CRC32";

    /**
     * Mask to be used by the wrapping algorithm.
     */
    private byte[] mask;

    /**
     * Checksum helper to use for checksum calculation.
     */
    private final CheckSumHelper checkSumHelper;

    /**
     * Use to manage wrapping of the mask for individual bytes.
     */
    private long counter = 0;

    /**
     * Filters the provided output stream to encapsulate the data.
     *
     * @param out
     *            - OutputStream to write to.
     * @param mask
     *            - The mask to use with this technique.
     * @param headerCheckSum
     *            - The type of checksum to use for the header. Use the static strings provided - CRC_32 or SHA_256.
     * @param bodyCheckSum
     *            - The type of checksum to use for the header. Use the static strings provided - CRC_32 or SHA_256.
     * @throws IOException
     *             - for any problems during wrapping.
     */
    public Technique1Wrapper(OutputStream out, byte[] mask, String headerCheckSum, String bodyCheckSum)
                    throws IOException {
        super(out);
        this.mask = mask.clone();

        // Create the technique needed for encapsulation.
        Technique encapsulation = new Technique(Technique.TECHNIQUE_1, mask);

        // Create header. If checksums aren't present, null is passed in and an empty Technique is used.
        Header header = new Header(encapsulation, headerCheckSum, bodyCheckSum);

        // Write the header to the stream ready for streaming of input.
        out.write(header.getHeaderBytes());

        if (CRC_32.equals(bodyCheckSum)) {
            checkSumHelper = new CRC32Helper();
        } else if (SHA_256.equals(bodyCheckSum)) {
            try {
                checkSumHelper = new SHA256Helper();
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException("Failed to find SHA256 Algorithm", e);
            }
        } else {
            checkSumHelper = null;
        }
    }

    /**
     * Wraps and writes the specified byte array to this output stream.
     *
     * @param bytes
     *            - byte array to be written.
     * @throws IOException
     *             - for problems during the write.
     * @see java.io.FilterOutputStream#write(byte[])
     */
    @Override
    public void write(byte[] bytes) throws IOException {
        for (int i = 0; i < bytes.length; i++) {
            write(bytes[i]);
        }
    }

    /**
     * Wraps and writes the specified byte to this output stream.
     *
     * @param item
     *            - byte to output (given as an int)
     * @throws IOException
     *             - for problems during the write.
     * @see java.io.FilterOutputStream#write(int)
     */
    @Override
    public void write(int item) throws IOException {
        out.write(item ^ mask[(int) (counter % mask.length)]);
        counter++;

        if (checkSumHelper != null) {
            // do some checksum calculation.
            checkSumHelper.updateCheckSum(item);
        }
    }

    /**
     * Wraps and writes <code>len</code> bytes from the specified <code>byte</code> array starting at offset
     * <code>off</code> to this output stream.
     *
     * @param src
     *            - byte array to be written.
     * @param off
     *            - offset for starting point in array.
     * @param len
     *            - the maximum number of bytes to write.
     * @throws IOException
     *             - for problems during the write.
     * @see java.io.FilterOutputStream#write(byte[], int, int)
     */
    @Override
    public void write(byte[] src, int off, int len) throws IOException {
        // Guard against invalid indexes to prevent corruption of output.
        if ((off | len | (src.length - (len + off)) | (off + len)) < 0) {
            throw new IndexOutOfBoundsException();
        }
        for (int i = 0; i < len; i++) {
            write(src[off + i]);
        }
    }

    /**
     * Close the output stream.
     *
     * @throws IOException
     *             - for problems during the close.
     * @see java.io.FilterOutputStream#close()
     */
    @Override
    public void close() throws IOException {
        if (checkSumHelper != null) {
            // Get the checksum.
            byte[] checksum = checkSumHelper.getCheckSum();

            // If it's a CRC32 (SHA256 will be longer).
            if (checksum.length == 8) {
                // We only want the last 4 bytes (helper will return 8 with 4 zeroes at the front).
                byte[] finalCheckSum = new byte[4];
                System.arraycopy(checksum, 4, finalCheckSum, 0, 4);

                write(finalCheckSum);
            } else {
                write(checksum);
            }
        }
        out.close();
    }
}
