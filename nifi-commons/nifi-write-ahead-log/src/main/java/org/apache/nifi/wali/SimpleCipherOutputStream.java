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

import org.bouncycastle.crypto.io.CipherOutputStream;
import org.bouncycastle.crypto.modes.AEADBlockCipher;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class extends {@link CipherOutputStream} with a static factory method for constructing
 * an output stream with an AEAD block cipher.
 *
 * Note that the {@link CipherOutputStream} implementation writes the MAC at the end of the stream during `close`.
 * If streams using this class aren't closed properly, the result may be a stream without a MAC written, which
 * causes a MAC authentication failure in the input stream.
 *
 */
public class SimpleCipherOutputStream extends CipherOutputStream {
    /**
     * Constructs an {@link OutputStream} from an existing {@link OutputStream} and block cipher.
     *
     * @param out output stream to wrap.
     * @param cipher block cipher, initialized for encryption.
     */
    public SimpleCipherOutputStream(OutputStream out, AEADBlockCipher cipher) {
        super(out, cipher);
    }

    /**
     * Static factory for wrapping an output stream with a block cipher.
     *
     * NB:  this function eagerly writes the initial cipher values to the plain output stream before returning the cipher stream.
     *
     * @param out output stream to wrap.
     * @param key cipher key.
     * @return wrapped output stream.
     * @throws IOException if the stream cannot be written eagerly, or if the cipher cannot be initialized.
     */
    public static OutputStream wrapWithKey(OutputStream out, SecretKey key) throws IOException {
        if (key == null) {
            return out;
        }

        byte[] iv = SimpleCipherUtil.createIV();
        byte[] aad = SimpleCipherUtil.createAAD();
        AEADBlockCipher cipher = SimpleCipherUtil.initCipher(key, true, iv, aad);

        // write our initial bits:
        out.write(SimpleCipherUtil.MARKER_BYTE);
        out.write(iv);
        out.write(aad);

        return new SimpleCipherOutputStream(out, cipher);
    }
}
