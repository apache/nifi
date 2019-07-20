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

import org.junit.Before;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;


class TestAbstractSimpleCipher {
    static SecureRandom random = new SecureRandom();

    SecretKey[] cipherKeys;
    SecretKey cipherKey;

    byte[] bigSecret;
    byte[] smallSecret;

    @Before
    public void setupSecretAndKey() {
        bigSecret = randomBytes(randomInt(1024*1024*10));
        smallSecret = randomBytes(randomInt(4096));

        cipherKeys = new SecretKey[4];
        cipherKeys[0] = null;
        cipherKeys[1] = new SecretKeySpec(randomBytes(32), SimpleCipherUtil.ALGO);
        cipherKeys[2] = new SecretKeySpec(randomBytes(16), SimpleCipherUtil.ALGO);
        cipherKeys[3] = null;

        cipherKey = cipherKeys[1];
    }

    static byte[] randomBytes(int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    static int randomInt(int size) {
        return random.nextInt(size);
    }

    static byte[] readAll(InputStream in, int size) throws IOException {
        byte[] dest = new byte[size];
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int len;
        while ((len = in.read(dest, 0, dest.length)) != -1) {
            buffer.write(dest, 0, len);
        }
        return buffer.toByteArray();
    }
}
