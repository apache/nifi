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

public class TestSimpleCipherInputStream extends TestAbstractSimpleCipher {
    @Test
    public void testCipherInputStream() throws IOException {
        // This shows we can use a variety of keys (some null) with the cipher streams.
        for (SecretKey cipherKey : cipherKeys) {
            ByteArrayOutputStream cipherByteOutputStream = new ByteArrayOutputStream();
            OutputStream outputStream = SimpleCipherOutputStream.wrapWithKey(cipherByteOutputStream, cipherKey);

            if (cipherKey == null) {
                Assert.assertNotEquals(SimpleCipherOutputStream.class, outputStream.getClass());
            } else {
                Assert.assertEquals(SimpleCipherOutputStream.class, outputStream.getClass());
            }

            outputStream.write(bigSecret);
            outputStream.close();

            byte[] cipherText = cipherByteOutputStream.toByteArray();
            ByteArrayInputStream cipherByteInputStream = new ByteArrayInputStream(cipherText);
            InputStream cipherInputStream = SimpleCipherInputStream.wrapWithKey(cipherByteInputStream, cipherKey);

            if (cipherKey == null) {
                Assert.assertNotEquals(SimpleCipherInputStream.class, cipherInputStream.getClass());
            } else {
                Assert.assertEquals(SimpleCipherInputStream.class, cipherInputStream.getClass());
            }

            byte[] plainText = readAll(cipherInputStream, bigSecret.length*2);
            Assert.assertArrayEquals(bigSecret, plainText);
        }
    }

    // This test shows that we cannot de-cipher modified (tampered, corrupted) cipher text.
    @Test
    public void testCipherInputStreamTampering() throws IOException {
        ByteArrayOutputStream cipherByteOutputStream = new ByteArrayOutputStream();
        OutputStream outputStream = SimpleCipherOutputStream.wrapWithKey(cipherByteOutputStream, cipherKey);

        outputStream.write(smallSecret);
        outputStream.close();

        byte[] cipherText = cipherByteOutputStream.toByteArray();
        int fails = 0;

        // This shows we can randomly tamper with (change) any single byte (except the first) and the result is that
        // the cipher will throw an exception during decryption:
        for (int i = 1; i < cipherText.length; i++) {
            byte[] cipherCopy = new byte[cipherText.length];
            System.arraycopy(cipherText, 0, cipherCopy, 0, cipherText.length);

            // tamper with the byte:
            cipherCopy[i] += 1 + random.nextInt(253);

            ByteArrayInputStream cipherByteInputStream = new ByteArrayInputStream(cipherCopy);
            InputStream cipherInputStream = SimpleCipherInputStream.wrapWithKey(cipherByteInputStream, cipherKey);
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();

            byte[] plainText = new byte[smallSecret.length*2];
            int len;

            try {
                while ((len = cipherInputStream.read(plainText, 0, plainText.length)) != -1) {
                    buffer.write(plainText, 0, len);
                }
            } catch (final Exception ignored) {
                fails += 1;
            }
        }

        // This shows we failed on every iteration (and skipped the first byte):
        Assert.assertEquals(fails, cipherText.length-1);
    }

    // This shows how a ciphered output stream interacts with a non-ciphered input stream.
    // When the non-ciphered input stream is constructed, the static factory method returns
    // the original stream because it was not passed a cipher.
    @Test
    public void testCipherTextOutputStreamPlainInputStream() throws IOException {
        ByteArrayOutputStream cipherByteOutputStream = new ByteArrayOutputStream();
        OutputStream outputStream = SimpleCipherOutputStream.wrapWithKey(cipherByteOutputStream, cipherKey);

        outputStream.write(bigSecret);
        outputStream.close();
        byte[] cipherText = cipherByteOutputStream.toByteArray();

        ByteArrayInputStream cipherByteInputStream = new ByteArrayInputStream(cipherText);
        InputStream inputStream = SimpleCipherInputStream.wrapWithKey(cipherByteInputStream, null);
        Assert.assertArrayEquals(cipherText, readAll(inputStream, cipherText.length));
    }

    // This shows how a non-ciphered output stream interacts with a ciphered input stream.
    // When the ciphered input stream does not see the marker byte, the static factory method
    // rewinds and returns the original input stream.
    @Test
    public void testPlainTextOutputStreamCipherTextInputStream() throws IOException {
        ByteArrayOutputStream cipherByteOutputStream = new ByteArrayOutputStream();
        OutputStream stream = SimpleCipherOutputStream.wrapWithKey(cipherByteOutputStream, null);

        stream.write(bigSecret);
        stream.close();
        byte[] cipherText = cipherByteOutputStream.toByteArray();

        ByteArrayInputStream cipherByteInputStream = new ByteArrayInputStream(cipherText);
        InputStream inputStream = SimpleCipherInputStream.wrapWithKey(cipherByteInputStream, cipherKey);
        Assert.assertArrayEquals(cipherText, readAll(inputStream, cipherText.length));
    }

    // This shows how input streams are handled when the input stream is so short that the cipher cannot initialize.
    // The current implementation returns the original input stream as-is.
    @Test
    public void testTruncatedInputs() throws IOException {
        int[] sizes = new int[]{
                0,
                1,
                2,
                4,
                SimpleCipherUtil.IV_BYTE_LEN - 1,
                SimpleCipherUtil.IV_BYTE_LEN + SimpleCipherUtil.AAD_BYTE_LEN - 1,
                4096,
        };

        for (int size : sizes) {
            byte[] cipherText = randomBytes(size);
            ByteArrayInputStream cipherByteInputStream = new ByteArrayInputStream(cipherText);
            InputStream inputStream = SimpleCipherInputStream.wrapWithKey(cipherByteInputStream, cipherKey);
            Assert.assertArrayEquals(cipherText, readAll(inputStream, cipherText.length));
        }
    }
}
