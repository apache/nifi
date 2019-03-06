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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.wrapping.common.CRC32Helper;
import org.apache.nifi.wrapping.common.CheckSumHelper;
import org.apache.nifi.wrapping.common.SHA256Helper;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.util.Arrays;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

//CHECKSTYLE.ON: CustomImportOrderCheck

/**
 * To perform unwrapping as an extension to the filter input stream.
 */
public class Unwrapper extends FilterInputStream {

    /**
     * Magic number to mark the beginning of the header.
     */
    public static final int START_MAGIC = 0xD1DF5FFF;

    /**
     * Magic number to mark the end of the header.
     */
    public static final int END_MAGIC = 0xFF5FDFD1;

    /**
     * Index in the header of the technique data.
     */
    private static final int TECHNIQUE_DATA_START_INDEX = 20;

    /**
     * Index in the header where we expect the data length.
     */
    private static final int DATA_LENGTH_START_INDEX = 18;

    /**
     * Initialisation vector used to set up Cipher object for Technique 2.
     */
    private static final byte[] INIT_IV_BYTES = new byte[16];

    static {
        Arrays.fill(INIT_IV_BYTES, (byte) 0);
    }

    /**
     * Index in the header where we expect to find the 4 byte section specifying the encapsulation to use.
     */
    private static final int ENCAPSULATION_START_INDEX = 12;

    // /**
    // * Index in the header where we expect to find the 2 byte section containing config information.
    // */
    // private static final int CONFIG_START_INDEX = 16; Not currently in use.

    /**
     * Index in the header where we expect to find the 4 byte section giving total header length.
     */
    private static final int HEADER_LENGTH_START_INDEX = 8;

    /**
     * Index in the header where we expect to find START_MAGIC.
     */
    private static final int START_MAGIC_NUMBER_INDEX = 0;

    /**
     * Represents the length of an integer when stored in a byte array.
     */
    private static final int INTEGER_BYTE_LENGTH = 4;

    /**
     * Represents the length of an integer when stored in a byte array.
     */
    private static final int SHORT_BYTE_LENGTH = 2;

    /**
     * Standard SHA256 checksum length.
     */
    private static final int SHA_CHECKSUM_LENGTH = 32;

    /**
     * Standard CRC checksum length.
     */
    private static final int CRC_CHECKSUM_LENGTH = 4;

    /**
     * The unwrapper used to provide the unwrap functionality.
     */
    private TechUnwrapper techUnwrapper;

    /**
     * True once enough header has been read to ascertain the size, otherwise false.
     */
    private boolean initiallySized = false;

    /**
     * Used during initial header parse.
     */
    private byte[] tempHeaderDetails = new byte[0];

    /**
     * The complete header.
     */
    private byte[] headerContents = new byte[0];

    /**
     * Configuration file.
     */
    private final File configFile;

    /**
     * This is used to limit the amount of the header we read initially - we initially need the first 12 bytes
     * regardless.
     */
    private int headerLength = 12;

    /**
     * The Stream for output.
     */
    private final OutputStream outputStream;

    /**
     * Constructor 1. Unwraps the provided input stream using a configuration file.
     *
     * @param in
     *            input stream to unwrap.
     * @param configFile
     *            file with configuration details
     * @throws IOException
     *             for any problems with initialising the input stream.
     */
    public Unwrapper(InputStream in, File configFile) throws IOException {
        super(in);
        this.configFile = configFile;
        this.outputStream = null;
        readInitialHeader(in);

    }

    /**
     * Constructor 2. Unwraps the provided input stream to a given output stream.
     *
     * @param in
     *            input stream to unwrap.
     * @param out
     *            output stream for the unwrapped output.
     * @throws IOException
     *             for any problems with initialising the input and output streams.
     */
    public Unwrapper(InputStream in, OutputStream out) throws IOException {
        super(in);
        this.configFile = null;
        this.outputStream = out;
        readInitialHeader(in);

    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.FilterInputStream#read(byte[])
     */
    @Override
    public int read(byte[] src) throws IOException {
        return techUnwrapper.read(src);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.FilterInputStream#read(byte[], int, int)
     */
    @Override
    public int read(byte[] src, int off, int len) throws IOException {
        return techUnwrapper.read(src, off, len);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.FilterInputStream#read()
     */
    @Override
    public int read() throws IOException {
        return techUnwrapper.read();
    }

    /**
     * Read in the initial header from the given input stream.
     *
     * @param in
     *            the input stream to read.
     * @throws IOException
     *             for problems reading from the input stream.
     */
    private void readInitialHeader(final InputStream in) throws IOException {
        while (headerContents.length < headerLength) {
            try {
                final int readByte = in.read();

                // Check that we're not at the end of the stream.
                if (readByte != -1) {
                    firstPassHeaderCheck(readByte);
                } else {
                    throw new RuntimeException("There was an error processing the header.");
                }
            } catch (IOException e) {
                throw new RuntimeException("There was an error reading from the supplied input stream.");
            }
        }
        // Now we have the header, process it to set up the correct decryption methods.
        setUpUnwrap();
    }

    /**
     * Initially read the 12 bytes to pull out some information we need before we can do anything else. As we'll have
     * read it, store in a temp array to be passed to the full setup method. The first 12 bytes written here will drop
     * out early, once we have 12 we'll do the processing.
     *
     * @param readByte
     *            the byte to process.
     */
    private void firstPassHeaderCheck(final int readByte) {
        // We haven't yet read enough of the header to get the length value, so continue.
        if (!initiallySized) { // We need the first 12 bytes so that we can get the "header length" value. This is 4
                               // bytes long, starting at index 8.
            if (tempHeaderDetails.length < headerLength) {
                tempHeaderDetails = ArrayUtils.add(tempHeaderDetails, (byte) readByte);
                // Drop out until we've read the 12 bytes.
                return;
            }
            tempHeaderDetails = ArrayUtils.add(tempHeaderDetails, (byte) readByte);

            // Check the first 4 bytes for the magic number.
            if (START_MAGIC != byteToInt(ArrayUtils.subarray(tempHeaderDetails, START_MAGIC_NUMBER_INDEX,
                            INTEGER_BYTE_LENGTH))) {
                throw new RuntimeException(
                                "Data submitted for processing was incorrectly formatted: Header start not present.");
            }

            // We have first 12 bytes- last 4 of these is header length.
            final ByteBuffer headerLengthCheck = ByteBuffer.allocate(INTEGER_BYTE_LENGTH);
            // Get the int value for the header length.
            headerLengthCheck.put(tempHeaderDetails, HEADER_LENGTH_START_INDEX, INTEGER_BYTE_LENGTH);
            headerLengthCheck.position(0);
            // Now set the header length to the actual value rather than the initial value.
            headerLength = headerLengthCheck.getInt();

            // Add the details to the header contents array.
            headerContents = ArrayUtils.addAll(headerContents, tempHeaderDetails);

            // Header now correctly sized.
            initiallySized = true;
        } else {
            // Add the next value to the end of the array.
            headerContents = ArrayUtils.add(headerContents, (byte) readByte);
        }
    }

    /**
     * Extract the information from the header, and set up the unwrapper for use.
     *
     * @throws IOException
     *             for problems reading from the input stream.
     */
    private void setUpUnwrap() throws IOException {
        // Length of checksum needed for buffering to get the body data.
        int bodyCheckSumLength = 0;

        // Encapsulation technique.
        int technique = byteToInt(ArrayUtils.subarray(headerContents, ENCAPSULATION_START_INDEX,
                        ENCAPSULATION_START_INDEX + INTEGER_BYTE_LENGTH));

        // Get data length.
        short dataLengthShort = byteToShort(ArrayUtils.subarray(headerContents, DATA_LENGTH_START_INDEX,
                        DATA_LENGTH_START_INDEX + SHORT_BYTE_LENGTH));

        // We probably don't need config here - we know data length. If it's only 4 bytes, we know its
        // the index to the file of keys (This only applies to tech 2!). Restrictions are in place
        // upstream to stop invalid values from Technique 2.
        final byte[] techniqueData;
        if (dataLengthShort == 4 && technique == 2) {
            // This is an index to the key, retrieve the value.
            int index = byteToInt(ArrayUtils.subarray(headerContents, TECHNIQUE_DATA_START_INDEX,
                            (TECHNIQUE_DATA_START_INDEX + dataLengthShort)));

            // Get the right line from the file.
            // We need to get the mask to use from the config file.
            try (BufferedReader maskReader = new BufferedReader(new InputStreamReader(
                            new FileInputStream(configFile), "UTF-8"))) {

                String keyString = null;

                // Loop through file until we've selected the correct line.
                for (int i = 1; i <= index; i++) {
                    keyString = maskReader.readLine();
                }

                // Convert this to a byte array to use to set up the Cipher.
                techniqueData = keyString.getBytes("UTF-8");
            } catch (FileNotFoundException e) {
                throw new IOException("Error occurred whilst trying to read from the supplied configuration file" + e);
            } // End Try-Release

        } else { // Otherwise just get the key as usual.
            // Now that we know the length of the technique data, we can retrieve it. Remember start index
            // is inclusive, end index is exclusive.
            techniqueData = ArrayUtils.subarray(headerContents, TECHNIQUE_DATA_START_INDEX,
                            (TECHNIQUE_DATA_START_INDEX + dataLengthShort));
        }

        int currentPosition = TECHNIQUE_DATA_START_INDEX + dataLengthShort;

        // There are now 16 bytes between currentPosition and the start of the magic number - two 8 byte
        // technique descriptions of checksums to use. We only care about the first 4 bytes of each of
        // these.
        final int headerCheckSumType = byteToInt(ArrayUtils.subarray(headerContents, currentPosition, currentPosition
                        + INTEGER_BYTE_LENGTH));

        // Position ourselves at the start of the body checksum.
        currentPosition = currentPosition + 2 * (INTEGER_BYTE_LENGTH);

        final int bodyCheckSumType = byteToInt(ArrayUtils.subarray(headerContents, currentPosition, currentPosition
                        + INTEGER_BYTE_LENGTH));
        currentPosition = currentPosition + 2 * (INTEGER_BYTE_LENGTH);

        CheckSumHelper checkSumHelper = null;
        if (bodyCheckSumType != 0) {
            if (bodyCheckSumType == 1) {
                // CRC checksums are in use for the body.
                checkSumHelper = new CRC32Helper();
                bodyCheckSumLength = CRC_CHECKSUM_LENGTH;

            }
            if (bodyCheckSumType == 2) {
                // SHA checksum are in use for the body.
                try {
                    checkSumHelper = new SHA256Helper();
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException("Failed to find SHA256 Algorithm", e);
                }
                bodyCheckSumLength = SHA_CHECKSUM_LENGTH;
            }
        }

        // Check end magic.
        int endMagic = byteToInt(ArrayUtils.subarray(headerContents, headerContents.length - INTEGER_BYTE_LENGTH,
                        headerContents.length));

        if (endMagic != END_MAGIC) {
            throw new RuntimeException(
                            "Data submitted for processing was incorrectly formatted: Header end not present.");
        }

        // Body checksum.
        byte[] bodyChecksum = new byte[bodyCheckSumLength];
        // If we have a header checksum, we need to retrieve it.
        if (headerCheckSumType != 0) {
            // We've already got the start position of the checksum. We know the last 4 bytes of the
            // header are the magic number, so the checksum is what lies between the two.
            final byte[] headerCheckSumBytes = ArrayUtils.subarray(headerContents, currentPosition,
                            headerContents.length - INTEGER_BYTE_LENGTH);

            // As we have got a checksum of the header, lets make sure everything is as it should be.
            // Checksum applies to the header without the END_MAGIC, so we need everything up to the end
            // of the last technique section.
            byte[] checkSumInput = ArrayUtils.subarray(headerContents, 0, currentPosition);

            // CRC 32
            if (headerCheckSumType == 1) {
                // Generate the CRC32 of the header we have received.
                Checksum checksum = new CRC32();
                checksum.update(checkSumInput, 0, checkSumInput.length);

                byte[] derivedChecksum = new byte[4];

                byte[] tempChecksum = ByteBuffer.allocate(8).putLong(checksum.getValue()).array();
                System.arraycopy(tempChecksum, 4, derivedChecksum, 0, 4);

                if (!Arrays.equals(derivedChecksum, headerCheckSumBytes)) {
                    throw new RuntimeException("Invalid header: "
                                    + "checksum provided does not match checksum derived during unwrapping process.");
                }
            }
            // SHA 256
            if (headerCheckSumType == 2) {
                byte[] ourCheckSum = null;

                try {
                    MessageDigest md = MessageDigest.getInstance("SHA-256");
                    md.update(checkSumInput, 0, checkSumInput.length);
                    ourCheckSum = md.digest();
                } catch (NoSuchAlgorithmException e) {
                    // This should never happen, as the algorithm to be used is hard coded.
                    throw new RuntimeException("Fatal error occurred while setting up check sum method: " + e);
                }
                if (!Arrays.equals(ourCheckSum, headerCheckSumBytes)) {
                    throw new RuntimeException("Invalid header: "
                                    + "checksum provided does not match checksum derived during unwrapping process.");
                }
            }
        }

        // Set up the buffered reader with the appropriate buffer size to deal with the checksum. Add 1
        // so that we can get the last character whilst retaining an amount of data equal to the length
        // of the checksum in use.
        BufferedInputStream bis = new BufferedInputStream(in, bodyCheckSumLength + 1);

        if (technique == 1) {
            // Perform final setup for technique 1.
            // Read checksum.length of data into the buffer to let us read ahead later.
            final int[] readBlock = new int[bodyCheckSumLength];

            for (int i = 0; i < readBlock.length; i++) {
                readBlock[i] = bis.read();
            }

            techUnwrapper = new Tech1Unwrapper(bis, outputStream, bodyCheckSumLength, bodyChecksum, readBlock,
                            checkSumHelper, techniqueData);
        } else if (technique == 2) {
            // Perform final setup for technique 2.
            Cipher cipher;
            CipherInputStream cipherIn = null;

            try {
                Security.addProvider(new BouncyCastleProvider());
                // Set up the AES cipher gubbins.
                // Get the key.
                SecretKeySpec key = new SecretKeySpec(techniqueData, "AES");
                IvParameterSpec ivSpec = new IvParameterSpec(INIT_IV_BYTES);

                // Create the cipher - specify BouncyCastle as algorithm provider.
                cipher = Cipher.getInstance("AES/CTR/NoPadding", "BC");
                cipher.init(Cipher.DECRYPT_MODE, key, ivSpec);
                cipherIn = new CipherInputStream(bis, cipher);
            } catch (GeneralSecurityException e) {
                // This should never happen, as the values are hardcoded. If we move towards allowing
                // ciphers to be specified manually this will need reworking.
                throw new RuntimeException("A fatal error occurred during setup of the cipher mechanism: " + e);
            }

            techUnwrapper = new Tech2Unwrapper(cipherIn, outputStream, bodyCheckSumLength, bodyChecksum,
                            checkSumHelper);
        }
    }

    /**
     * Convert 2 bytes into an short.
     *
     * @param bytes
     *            array to convert from.
     * @return a short value taken from top two bytes of given array.
     */
    private short byteToShort(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.put(bytes);
        buffer.position(0);

        return buffer.getShort();
    }

    /**
     * Convert 4 bytes into an int.
     *
     * @param bytes
     *            array to convert from.
     * @return an int value taken from top four bytes of given array.
     */
    private int byteToInt(byte[] bytes) {
        final ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.put(bytes);
        buffer.position(0);

        return buffer.getInt();
    }
}