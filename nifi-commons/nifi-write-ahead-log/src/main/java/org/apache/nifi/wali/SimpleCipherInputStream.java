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
package org.apache.nifi.wali;

import org.apache.nifi.stream.io.StreamUtils;
import org.bouncycastle.crypto.io.CipherInputStream;
import org.bouncycastle.crypto.io.InvalidCipherTextIOException;
import org.bouncycastle.crypto.modes.AEADBlockCipher;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class extends {@link CipherInputStream} with a static factory method for constructing
 * an input stream with an AEAD block cipher.
 */
public class SimpleCipherInputStream extends CipherInputStream {
    protected AEADBlockCipher cipher;

    /**
     * Constructs an {@link InputStream} from an existing {@link InputStream} and block cipher.
     *
     * @param in input stream to wrap.
     * @param cipher block cipher, initialized for decryption.
     */
    public SimpleCipherInputStream(InputStream in, AEADBlockCipher cipher) {
        super(in, cipher);
        this.cipher = cipher;
    }

    /**
     * Constructs an {@link InputStream} from an existing {@link InputStream} and secret key.
     *
     * This constructor eagerly reads the initial cipher values from the plain input stream during cipher initialization.
     *
     * @param in input stream to wrap.
     * @param key cipher key.
     * @throws IOException if the stream cannot be read eagerly, or if the cipher cannot be initialized.
     */
    public SimpleCipherInputStream(InputStream in, SecretKey key) throws IOException {
        super(in, createCipherAndReadHeader(in, key));
    }

    private static AEADBlockCipher createCipherAndReadHeader(InputStream in, SecretKey key) throws IOException {
        if (in.markSupported()) {
            in.mark(0);
        }

        final int marker = in.read();
        if (marker != SimpleCipherUtil.MARKER_BYTE) {
            throw new IOException("Input stream not cipher");
        }


        byte[] iv = new byte[SimpleCipherUtil.IV_BYTE_LEN];
        if (StreamUtils.fillBuffer(in, iv, true) != iv.length) {
            throw new IOException("Could not read IV.");
        }

        byte[] aad = new byte[SimpleCipherUtil.AAD_BYTE_LEN];
        if (StreamUtils.fillBuffer(in, aad, true) != aad.length) {
            throw new IOException("Could not read AAD.");
        }
        return SimpleCipherUtil.initCipher(key, false, iv, aad);
    }

    /**
     * Peeks at the next value in the input stream, returning true if that value matches our cipher stream marker.
     * @param in InputStream to check
     * @return false if input stream is not markable or does not have first byte marker
     * @throws IOException when the input stream throws {@link IOException}
     */
    public static boolean peekForMarker(InputStream in) throws IOException {
        if (!in.markSupported())
            return false;

        in.mark(0);
        final int marker = in.read();
        in.reset();
        return marker == SimpleCipherUtil.MARKER_BYTE;
    }

    @Override
    public void close() throws IOException {
        try {
            in.close();
        } catch (final InvalidCipherTextIOException e) {
            throw new IOException(e);
        }
    }
}
