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

import org.junit.Assert;
import org.junit.Test;

import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Basic test for the cipher output stream; see {@link TestSimpleCipherInputStream} for the majority of the stream tests.
 */
public class TestSimpleCipherOutputStream extends TestAbstractSimpleCipher {
    @Test
    public void testCipherOutputStream() throws IOException {
        // This shows that the output stream works as expected with a variety of cipher keys.
        for (SecretKey cipherKey : cipherKeys) {
            ByteArrayOutputStream cipherByteOutputStream = new ByteArrayOutputStream();
            OutputStream outputStream = SimpleCipherOutputStream.wrapWithKey(cipherByteOutputStream, cipherKey);

            outputStream.write(bigSecret);
            outputStream.close();

            byte[] cipherText = cipherByteOutputStream.toByteArray();
            ByteArrayInputStream cipherByteInputStream = new ByteArrayInputStream(cipherText);
            InputStream inputStream = SimpleCipherInputStream.wrapWithKey(cipherByteInputStream, cipherKey);

            byte[] plainText = readAll(inputStream, bigSecret.length * 2);
            Assert.assertArrayEquals(bigSecret, plainText);
        }
    }
}