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

import org.apache.nifi.wrapping.common.CRC32Helper;
import org.apache.nifi.wrapping.common.CheckSumHelper;
import org.apache.nifi.wrapping.common.SHA256Helper;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

//CHECKSTYLE.ON: CustomImportOrderCheck

/**
 * Wraps a stream of data using technique 2.
 */
public class Technique2Wrapper extends FilterOutputStream implements Wrapper {
    /**
     * Specify SHA256 checksum use.
     */
    public static final String SHA_256 = "SHA256";

    /**
     * Specify CRC32 checksum use.
     */
    public static final String CRC_32 = "CRC32";

    /**
     * Charset to use for byte encoding.
     */
    private static final String USE_CHARSET = "UTF-8";

    /**
     * Initial value for IV.
     */
    private static final byte[] INIT_IV_BYTES = new byte[16];

    static {
        Arrays.fill(INIT_IV_BYTES, (byte) 0);
    }

    /**
     * Use this for the AES encryption.
     */
    private Cipher cipher;

    /**
     * Checksum helper to use for checksum calculation.
     */
    private final CheckSumHelper checkSumHelper;

    /**
     * Output stream we'll use to wrap the outgoing stream.
     */
    private CipherOutputStream cipherOut;

    /**
     * Constructor 1. Filters the provided output stream to encapsulate the data.
     *
     * @param out
     *            - OutputStream to write to.
     * @param mask
     *            - The key to use with this technique. THIS MUST BE MAX 128 bits until extra policy jars are added.
     * @param headerCheckSum
     *            - The type of checksum to use for the header. Use the static strings provided - CRC_32 or SHA_256.
     * @param bodyCheckSum
     *            - The type of checksum to use for the header. Use the static strings provided - CRC_32 or SHA_256.
     * @throws IOException
     *             - for any problems during wrapping.
     */
    public Technique2Wrapper(OutputStream out, byte[] mask, String headerCheckSum, String bodyCheckSum)
                    throws IOException {
        super(out);

        // Create the technique needed for encapsulation.
        Technique encapsulation = new Technique(Technique.TECHNIQUE_2, mask);

        // Create header. If checksums aren't present, null is passed in and an empty Technique is used.
        Header header = new Header(encapsulation, headerCheckSum, bodyCheckSum);

        // Write the header to the stream ready for streaming of input.
        byte[] headerBytes = header.getHeaderBytes();

        for (int i = 0; i < headerBytes.length; i++) {
            out.write(headerBytes[i]);
        }

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
        try {
            Security.addProvider(new BouncyCastleProvider());
            // Set up the AES cipher gubbins.
            // Get the key.
            SecretKeySpec key = new SecretKeySpec(mask, "AES");
            IvParameterSpec ivSpec = new IvParameterSpec(INIT_IV_BYTES);

            // Create the cipher - specify BouncyCastle as algorithm provider.
            cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC");
            cipher.init(Cipher.ENCRYPT_MODE, key, ivSpec);
            cipherOut = new CipherOutputStream(out, cipher);
        } catch (GeneralSecurityException e) {
            // Should never happen as algorithm details are hardcoded.
            throw new RuntimeException("A fatal error occurred while setting up cipher mechanism");
        }
    }

    /**
     * Constructor 2. Filters the provided output stream to encapsulate the data - using a mask config file.
     *
     * @param out
     *            - OutputStream to write to.
     * @param maskConfig
     *            - The file with the mask config.
     * @param maskSelector
     *            - the mask index number to use from the config file.
     * @param headerCheckSum
     *            - The type of checksum to use for the header. Use the static strings provided - CRC_32 or SHA_256.
     * @param bodyCheckSum
     *            - The type of checksum to use for the header. Use the static strings provided - CRC_32 or SHA_256.
     * @throws IOException
     *             - for any problems during wrapping.
     */
    public Technique2Wrapper(OutputStream out, File maskConfig, int maskSelector, String headerCheckSum,
                    String bodyCheckSum) throws IOException {
        super(out);

        // We need to get the mask to use from the config file.
        String keyString = null;
        try (BufferedReader bis = new BufferedReader(new InputStreamReader(new FileInputStream(maskConfig),
                        USE_CHARSET))) {
            // Loop through file until we've selected the correct line.
            for (int i = 1; i <= maskSelector; i++) {
                keyString = bis.readLine();
            }
        } // End Try-Release

        // Convert this to a byte array to use to set up the Cipher.
        byte[] mask = keyString.getBytes(USE_CHARSET);

        // We need to convert the maskSelector int into a 4 byte array to pass to the header.
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(maskSelector);
        // Need network byte order.
        buffer.order(ByteOrder.BIG_ENDIAN);

        byte[] keyMaskBytes = buffer.array();

        // Now set everything up.
        // Create the technique needed for encapsulation.
        Technique encapsulation = new Technique(Technique.TECHNIQUE_2, keyMaskBytes);

        // Create header. If checksums aren't present, null is passed in and an empty Technique is used.
        Header header = new Header(encapsulation, headerCheckSum, bodyCheckSum);

        // Write the header to the stream ready for streaming of input.
        byte[] headerBytes = header.getHeaderBytes();

        for (int i = 0; i < headerBytes.length; i++) {
            out.write(headerBytes[i]);
        }

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
        try {
            Security.addProvider(new BouncyCastleProvider());
            // Set up the AES cipher gubbins.
            // Get the key.
            SecretKeySpec key = new SecretKeySpec(mask, "AES");
            IvParameterSpec ivSpec = new IvParameterSpec(INIT_IV_BYTES);

            // Create the cipher - specify BouncyCastle as algorithm provider.
            cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC");
            cipher.init(Cipher.ENCRYPT_MODE, key, ivSpec);
            cipherOut = new CipherOutputStream(out, cipher);
        } catch (GeneralSecurityException e) {
            // Should never happen as algorithm details are hardcoded.
            throw new RuntimeException("A fatal error occurred while setting up cipher mechanism");
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
        cipherOut.write(bytes);

        if (checkSumHelper != null) {
            // do some checksum calculation.
            checkSumHelper.updateCheckSum(bytes);
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
        // The header has been written.
        // Now use the CipherOutputStream to write the data out for encryption.
        cipherOut.write(item);

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

        byte[] copy = new byte[len];
        System.arraycopy(src, off, copy, 0, len);

        write(copy);
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
        cipherOut.close();
    }
}
