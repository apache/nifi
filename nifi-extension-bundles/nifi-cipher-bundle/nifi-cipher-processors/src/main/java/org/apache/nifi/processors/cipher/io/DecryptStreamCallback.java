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
package org.apache.nifi.processors.cipher.io;

import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.cipher.CipherException;

import javax.crypto.Cipher;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.spec.AlgorithmParameterSpec;

/**
 * Decrypt Stream Callback implements shared methods for cipher operations and parameter buffer processing
 */
public abstract class DecryptStreamCallback implements StreamCallback {

    private static final int BUFFER_LENGTH = 4096;

    private static final int BUFFER_INPUT_OFFSET = 0;

    private static final int END_OF_FILE = -1;

    private final Cipher cipher;

    private final int parameterBufferLength;

    public DecryptStreamCallback(
            final Cipher cipher,
            final int parameterBufferLength
    ) {
        this.cipher = cipher;
        this.parameterBufferLength = parameterBufferLength;
    }

    /**
     * Process streams with parsed algorithm parameters and cipher initialization
     *
     * @param inputStream Stream of encrypted bytes
     * @param outputStream Stream of decrypted bytes
     * @throws IOException Thrown on read or write failures
     */
    @Override
    public void process(final InputStream inputStream, final OutputStream outputStream) throws IOException {
        final ByteBuffer parameterBuffer = readParameterBuffer(inputStream);
        final AlgorithmParameterSpec algorithmParameterSpec = readAlgorithmParameterSpec(parameterBuffer);
        final Key key = getKey(algorithmParameterSpec);
        initCipher(key, algorithmParameterSpec);

        processBuffer(parameterBuffer, outputStream);
        processStream(inputStream, outputStream);
    }

    /**
     * Read Cipher Algorithm Parameters from initial buffer of parameter bytes
     *
     * @param parameterBuffer Buffer of parameter bytes
     * @return Algorithm Parameters Specification
     */
    protected abstract AlgorithmParameterSpec readAlgorithmParameterSpec(final ByteBuffer parameterBuffer);

    /**
     * Get Key for Cipher operations using Algorithm Parameters when needed
     *
     * @param algorithmParameterSpec Algorithm Parameters Specification
     * @return Key for decryption processing
     */
    protected abstract Key getKey(final AlgorithmParameterSpec algorithmParameterSpec);

    private void initCipher(final Key key, final AlgorithmParameterSpec algorithmParameterSpec) {
        try {
            cipher.init(Cipher.DECRYPT_MODE, key, algorithmParameterSpec);
        } catch (final GeneralSecurityException e) {
            final String message = String.format("Cipher [%s] initialization failed", cipher.getAlgorithm());
            throw new CipherException(message, e);
        }
    }

    private ByteBuffer readParameterBuffer(final InputStream inputStream) throws IOException {
        final byte[] buffer = new byte[parameterBufferLength];
        final int read = inputStream.read(buffer);
        if (read == END_OF_FILE) {
            throw new EOFException("Read parameters buffer failed");
        }
        return ByteBuffer.wrap(buffer, BUFFER_INPUT_OFFSET, read);
    }

    private void processBuffer(final ByteBuffer byteBuffer, final OutputStream outputStream) throws IOException {
        if (byteBuffer.hasRemaining()) {
            final int remaining = byteBuffer.remaining();
            final byte[] buffer = new byte[remaining];
            byteBuffer.get(buffer);
            processBytes(buffer, remaining, outputStream);
        }
    }

    private void processBytes(final byte[] buffer, final int length, final OutputStream outputStream) throws IOException {
        final byte[] deciphered = cipher.update(buffer, BUFFER_INPUT_OFFSET, length);
        if (deciphered != null) {
            outputStream.write(deciphered);
        }
    }

    private void processStream(final InputStream inputStream, final OutputStream outputStream) throws IOException {
        final byte[] buffer = new byte[BUFFER_LENGTH];
        int read;
        while ((read = inputStream.read(buffer)) != END_OF_FILE) {
            processBytes(buffer, read, outputStream);
        }
        try {
            final byte[] deciphered = cipher.doFinal();
            outputStream.write(deciphered);
        } catch (final GeneralSecurityException e) {
            final String message = String.format("Cipher [%s] verification failed", cipher.getAlgorithm());
            throw new CipherException(message, e);
        }
    }
}
