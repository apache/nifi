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

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import javax.crypto.CipherInputStream;

import org.apache.nifi.wrapping.common.CheckSumHelper;

/**
 * Unwraps Technique 2 data.
 */
public class Tech2Unwrapper implements TechUnwrapper {
    /**
     * CipherInputStream used to wrap the input stream.
     */
    private final CipherInputStream cipherIn;

    /**
     * Length of the body checksum.
     */
    private final int bodyCheckSumLength;

    /**
     * The actual body checksum.
     */
    private byte[] checksum;

    /**
     * The last bytes.
     */
    private byte[] lastbytes;

    /**
     * Checksum helper to use.
     */
    private final CheckSumHelper checkSumHelper;

    /**
     * The output stream.
     */
    private final OutputStream out;

    /**
     * Creates an unwrapper for technique 2 data.
     *
     * @param cipherIn
     *            - Input stream to use.
     * @param out
     *            - Output stream to use.
     * @param bodyCheckSumLength
     *            - The length of the body checksum.
     * @param checksum
     *            - checksum byte array.
     * @param checkSumHelper
     *            - Helper object.
     */
    protected Tech2Unwrapper(CipherInputStream cipherIn, OutputStream out, int bodyCheckSumLength, byte[] checksum,
                    CheckSumHelper checkSumHelper) {
        this.cipherIn = cipherIn;
        this.bodyCheckSumLength = bodyCheckSumLength;
        this.checksum = checksum.clone();
        this.checkSumHelper = checkSumHelper;
        this.out = out;
    }

    /*
     * (non-Javadoc)
     *
     * @see glib.app.fc.wrapping.unwrapper.TechUnwrapper#read()
     */
    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException("For performance reasons, single byte read is not supported. "
                        + "Please only attempt to unwrap multiple bytes at a time");
    }

    /*
     * (non-Javadoc)
     *
     * @see glib.app.fc.wrapping.unwrapper.TechUnwrapper#read(byte[], int, int)
     */
    @Override
    public int read(byte[] src, int off, int len) throws IOException {
        final byte[] write = new byte[len];
        System.arraycopy(src, off, write, 0, len);
        return read(write);
    }

    /*
     * (non-Javadoc)
     *
     * @see glib.app.fc.wrapping.unwrapper.TechUnwrapper#read(byte[])
     */
    @Override
    public int read(byte[] src) throws IOException {
        // No checksums, easy.
        if (bodyCheckSumLength == 0) {
            final int bytesRead = cipherIn.read(src);

            if (bytesRead == -1) {
                out.flush();
                return -1;
            }
            out.write(src, 0, bytesRead);

            return bytesRead;
        }

        // Read in the amount of data required from the crypt stream into b;
        final int bytesRead = cipherIn.read(src);

        // If the stream has ended. We'll have the checksum saved and not yet written out.
        if (bytesRead == -1) {
            out.flush();
            checksum = lastbytes;

            final byte[] finalCheckSum;

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

            return -1;
        }

        // first run - read in and keep the checksum buffer.
        if (lastbytes == null) {
            lastbytes = new byte[bodyCheckSumLength];

            // We've read in something - stash the last (bodyChecksum.length) bytes.
            System.arraycopy(src, (bytesRead - bodyCheckSumLength), lastbytes, 0, bodyCheckSumLength);

            // Write out the bytes minus last bytes.
            out.write(src, 0, bytesRead - bodyCheckSumLength);

            // <expensive>!
            final byte[] test = new byte[bytesRead - bodyCheckSumLength];
            System.arraycopy(src, 0, test, 0, bytesRead - bodyCheckSumLength);
            checkSumHelper.updateCheckSum(test);
            // </expensive!>

            // Done for now.
            return bytesRead;
        }

        // We've got at least enough bytes to continue - write out the checksum buffer, then replace it.
        if (bytesRead >= bodyCheckSumLength) {
            // Update checksum, write out buffer.
            checkSumHelper.updateCheckSum(lastbytes);
            out.write(lastbytes);

            // We've read in something - stash the last (bodyChecksum.length) bytes.
            System.arraycopy(src, (bytesRead - bodyCheckSumLength), lastbytes, 0, bodyCheckSumLength);
            // Write out the bytes minus last bytes.
            out.write(src, 0, bytesRead - bodyCheckSumLength);

            // <expensive>!
            final byte[] test = new byte[bytesRead - bodyCheckSumLength];
            System.arraycopy(src, 0, test, 0, bytesRead - bodyCheckSumLength);
            checkSumHelper.updateCheckSum(test);
            // </expensive!>

            // Done for now.
            return bytesRead;
        }

        // We need to be careful if what we have read is less than the length of the checksum.
        if (bytesRead < bodyCheckSumLength) {
            // Local checksum.
            final byte[] checksumLocal = new byte[bodyCheckSumLength];

            // Find the difference.
            int difference = bodyCheckSumLength - bytesRead;

            // We need to get difference bytes from saved array.
            System.arraycopy(lastbytes, (lastbytes.length - difference), checksumLocal, 0, difference);

            // Add the rest in from b.
            System.arraycopy(src, 0, checksumLocal, difference, bytesRead);

            // Make sure we write out the first bit of last bytes that we don't need to keep.
            out.write(lastbytes, 0, lastbytes.length - difference);

            // <expensive>!
            final byte[] test = new byte[(lastbytes.length - difference)];
            System.arraycopy(lastbytes, 0, test, 0, (lastbytes.length - difference));
            checkSumHelper.updateCheckSum(test);
            // </expensive!>

            // Although we could be at the end of the file,
            // there are occasions when there are more bytes to be read in.
            // Example: original file of size 497 bytes before wrapping;
            // when wrapped file is read in here it is read in as 512 bytes; then 16 bytes; then 1 byte.
            // So copy local checksum to lastbytes - let the (bytesRead == -1) above deal with end of file.
            System.arraycopy(checksumLocal, 0, lastbytes, 0, bodyCheckSumLength);
        }
        return bytesRead;
    }
}
