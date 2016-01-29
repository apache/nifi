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
package org.apache.nifi.processors.standard.util.crypto;

import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.slf4j.Logger;

import javax.crypto.Cipher;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public abstract class RandomIVPBECipherProvider implements PBECipherProvider {
    static final byte[] SALT_DELIMITER = "NiFiSALT".getBytes(StandardCharsets.UTF_8);
    static final int MAX_SALT_LIMIT = 128;
    static final byte[] IV_DELIMITER = "NiFiIV".getBytes(StandardCharsets.UTF_8);
    // This is 16 bytes for AES but can vary for other ciphers
    static final int MAX_IV_LIMIT = 16;

    /**
     * Returns an initialized cipher for the specified algorithm. The key is derived by the KDF of the implementation. The IV is provided externally to allow for non-deterministic IVs, as IVs
     * deterministically derived from the password are a potential vulnerability and compromise semantic security. See
     * <a href="http://crypto.stackexchange.com/a/3970/12569">Ilmari Karonen's answer on Crypto Stack Exchange</a>
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the salt
     * @param iv               the IV
     * @param keyLength        the desired key length in bits
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    abstract Cipher getCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, byte[] iv, int keyLength, boolean encryptMode) throws Exception;

    abstract Logger getLogger();

    private byte[] readBytesFromInputStream(InputStream in, String label, int limit, int minimum, byte[] delimiter) throws IOException, ProcessException {
        if (in == null) {
            throw new IllegalArgumentException("Cannot read " + label + " from null InputStream");
        }

        // If the value is not detected within the first n bytes, throw an exception
        in.mark(limit);

        // The first n bytes of the input stream contain the value up to the custom delimiter
        if (in.available() < minimum) {
            throw new ProcessException("The cipher stream is too small to contain the " + label);
        }
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        byte[] stoppedBy = StreamUtils.copyExclusive(in, bytesOut, limit + delimiter.length, delimiter);

        if (stoppedBy != null) {
            byte[] bytes = bytesOut.toByteArray();
            getLogger().info("[REMOVE] Read {}: {}", label, Hex.encodeHexString(bytes));
            return bytes;
        }

        // If no delimiter was found, reset the cursor
        getLogger().warn("No {} read from cipher input stream", label);
        in.reset();
        return null;
    }

    private void writeBytesToOutputStream(OutputStream out, byte[] value, String label, byte[] delimiter) throws IOException {
        if (out == null) {
            throw new IllegalArgumentException("Cannot write " + label + " to null OutputStream");
        }
        getLogger().info("[REMOVE] Writing {} {} and delimiter {}", label, Hex.encodeHexString(value), Hex.encodeHexString(delimiter));
        out.write(value);
        out.write(delimiter);
    }

    @Override
    public byte[] readSalt(InputStream in) throws IOException, ProcessException {
       return readBytesFromInputStream(in, "salt", MAX_SALT_LIMIT, getDefaultSaltLength(), SALT_DELIMITER);
    }

    @Override
    public void writeSalt(byte[] salt, OutputStream out) throws IOException {
       writeBytesToOutputStream(out, salt, "salt", SALT_DELIMITER);
    }

    public byte[] readIV(InputStream in) throws IOException, ProcessException {
        return readBytesFromInputStream(in, "IV", MAX_IV_LIMIT, MAX_IV_LIMIT, IV_DELIMITER);
    }

    public void writeIV(byte[] iv, OutputStream out) throws IOException {
       writeBytesToOutputStream(out, iv, "IV", IV_DELIMITER);
    }
}
