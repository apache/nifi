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
package org.apache.nifi.security.util.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.crypto.Cipher;
import org.apache.nifi.security.util.EncryptionMethod;

public interface PBECipherProvider extends CipherProvider {

    /**
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived by the KDF of the implementation.
     * <p/>
     * The IV can be retrieved by the calling method using {@link Cipher#getIV()}.
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param password         the secret input
     * @param salt             the salt
     * @param keyLength        the desired key length in bits
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    Cipher getCipher(EncryptionMethod encryptionMethod, String password, byte[] salt, int keyLength, boolean encryptMode) throws Exception;

    /**
     * Returns a random salt suitable for this cipher provider.
     *
     * @return a random salt
     * @see PBECipherProvider#getDefaultSaltLength()
     */
    byte[] generateSalt();

    /**
     * Returns the default salt length for this implementation.
     *
     * @return the default salt length in bytes
     */
    int getDefaultSaltLength();

    /**
     * Returns the salt provided as part of the cipher stream, or throws an exception if one cannot be detected.
     *
     * @param in the cipher InputStream
     * @return the salt
     */
    byte[] readSalt(InputStream in) throws IOException;

    /**
     * Writes the salt provided as part of the cipher stream, or throws an exception if it cannot be written.
     *
     * @param salt the salt
     * @param out  the cipher OutputStream
     */
    void writeSalt(byte[] salt, OutputStream out) throws IOException;
}
