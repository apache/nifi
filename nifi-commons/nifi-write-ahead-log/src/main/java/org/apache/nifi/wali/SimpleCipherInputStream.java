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

import org.bouncycastle.crypto.io.CipherInputStream;
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
     * Static factory for wrapping an input stream with a block cipher.
     *
     * NB:  this function eagerly reads the initial cipher values from the plain input stream before returning the cipher stream.
     *
     * @param in input stream to wrap.
     * @param key cipher key.
     * @return wrapped input stream.
     * @throws IOException if the stream cannot be read eagerly, or if the cipher cannot be initialized.
     */
    public static InputStream wrapWithKey(InputStream in, SecretKey key) throws IOException {
        if (key == null) {
            return in;
        }

        if (in.markSupported()) {
            in.mark(0);
        }

        // Read the marker, the iv, and the aad in the same order as they're written in the SimpleCipherOutputStream:
        try {
            final int marker = in.read();
            if (marker != SimpleCipherUtil.MARKER_BYTE) {
                if (in.markSupported()) {
                    in.reset();
                }
                return in;
            }

            byte[] iv = new byte[SimpleCipherUtil.IV_BYTE_LEN];
            int len = in.read(iv);
            if (len != iv.length) {
                throw new IOException("Could not read IV.");
            }

            byte[] aad = new byte[SimpleCipherUtil.AAD_BYTE_LEN];
            len = in.read(aad);
            if (len != aad.length) {
                throw new IOException("Could not read AAD.");
            }

            AEADBlockCipher cipher = SimpleCipherUtil.initCipher(key, false, iv, aad);
            return new SimpleCipherInputStream(in, cipher);

        } catch (final IOException ignored) {
            if (in.markSupported()) {
                in.reset();
            }
            return in;
        }
    }

    @Override
    public void close() throws IOException {
        try {
            in.close();
        } catch (final Exception e) {
            throw new IOException(e);
        }
    }
}
