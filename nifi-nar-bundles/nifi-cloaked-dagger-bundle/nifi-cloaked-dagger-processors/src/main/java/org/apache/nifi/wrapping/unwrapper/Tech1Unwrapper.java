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
package org.apache.nifi.wrapping.unwrapper;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.nifi.wrapping.common.CheckSumHelper;

/**
 * Unwraps Technique 1 data.
 */
public class Tech1Unwrapper implements TechUnwrapper {
    /**
     * Input stream to unwrap.
     */
    private final BufferedInputStream bis;

    /**
     * Length of the body checksum.
     */
    private final int bodyCheckSumLength;

    /**
     * The actual body checksum.
     */
    private final byte[] checksum;

    /**
     * Temporary storage for checksum calculation.
     */
    private final int[] readBlock;

    /**
     * Checksum helper to use.
     */
    private final CheckSumHelper checkSumHelper;

    /**
     * The fixed mask for technique 1.
     */
    private final byte[] techniqueData;

    /**
     * The output stream.
     */
    private final OutputStream out;

    /**
     * Used for position checking for checksum calculation.
     */
    private long pos = 0;

    /**
     * Used for checksum calculation.
     */
    private long counter = 0;

    /**
     * Creates an unwrapper for Technique 1 data.
     *
     * @param bis
     *            - Input Stream to use.
     * @param bodyCheckSumLength
     *            - The length of the body checksum.
     * @param checksum
     *            - checksum byte array.
     * @param readBlock
     *            - read block int array.
     * @param checkSumHelper
     *            - Helper object.
     * @param techniqueData
     *            - Technique byte array.
     */
    protected Tech1Unwrapper(BufferedInputStream bis, OutputStream out, int bodyCheckSumLength, byte[] checksum,
                    int[] readBlock, CheckSumHelper checkSumHelper, byte[] techniqueData) {
        this.bis = bis;
        this.bodyCheckSumLength = bodyCheckSumLength;
        this.checksum = checksum.clone();
        this.readBlock = readBlock.clone();
        this.checkSumHelper = checkSumHelper;
        this.techniqueData = techniqueData.clone();
        this.out = out;
    }

    /*
     * (non-Javadoc)
     *
     * @see glib.app.fc.wrapping.unwrapper.TechUnwrapper#read()
     */
    @Override
    public int read() throws IOException {
        final int num = bis.read();

        int ret = -1;

        // We've reached the end of the stream.
        if (num == -1) {
            // Get the checksum from the circular buffer.
            for (int i = 0; i < bodyCheckSumLength; i++) {
                final int rbIndex = (int) (pos % bodyCheckSumLength);
                final int tdIndex = (int) (counter % techniqueData.length);
                final int dec = (readBlock[rbIndex] ^ techniqueData[tdIndex]) & 0xFF;

                checksum[i] = ((byte) dec);

                counter++;
                readBlock[(int) (pos % bodyCheckSumLength)] = num;
                pos++;
            }

            if (bodyCheckSumLength != 0) {
                byte[] finalCheckSum = null;

                if (bodyCheckSumLength <= 8) {
                    // If we're dealing with a CRC32 we need to strip away the first 4 empty bytes.
                    finalCheckSum = new byte[4];
                    System.arraycopy(checkSumHelper.getCheckSum(), 4, finalCheckSum, 0, 4);
                } else {
                    finalCheckSum = checkSumHelper.getCheckSum();
                }

                // We are finished. Look at the checksum.
                if (!Arrays.equals(checksum, finalCheckSum)) {
                    // Something isn't right - checksums for body data do not match. die!
                    throw new RuntimeException("Checksum for received data was not as expected");
                }
            }
            // We've finished.
            out.flush();
            return -1;
        } else { // We've still got data to read.
            // No body checksum in use
            if (bodyCheckSumLength == 0) {
                int value = (num ^ techniqueData[(int) (counter % techniqueData.length)]);
                counter++;
                out.write(value & 0xFF);
                return 1;
            } else { // We need the circular buffer to keep track of the checksum.
                // Get the value to return.
                final int rbIndex = (int) (pos % bodyCheckSumLength);
                final int tdIndex = (int) (counter % techniqueData.length);
                ret = (readBlock[rbIndex] ^ techniqueData[tdIndex]) & 0xFF;
                counter++;
                readBlock[(int) (pos % bodyCheckSumLength)] = num;

                pos++;
            }
        }
        // If we're using a checksum, update it.
        if (bodyCheckSumLength != 0) {
            checkSumHelper.updateCheckSum(ret);
        }
        out.write(ret);

        return 1;
    }

    /*
     * (non-Javadoc)
     *
     * @see glib.app.fc.wrapping.unwrapper.TechUnwrapper#read(byte[], int, int)
     */
    @Override
    public int read(byte[] src, int off, int len) throws IOException {
        // create the array.
        byte[] copy = new byte[len];
        System.arraycopy(src, off, copy, 0, len);
        read(copy);
        return len;
    }

    /*
     * (non-Javadoc)
     *
     * @see glib.app.fc.wrapping.unwrapper.TechUnwrapper#read(byte[])
     */
    @Override
    public int read(byte[] src) throws IOException {
        // Attempt to read b.length from the stream.
        for (int i = 0; i < src.length; i++) {
            int returned = read();

            // If we hit the end of the stream.
            if (returned == -1) {
                out.flush();
                return -1;
            }
        }
        // Otherwise we've successfully read this many bytes.
        return src.length;
    }
}
