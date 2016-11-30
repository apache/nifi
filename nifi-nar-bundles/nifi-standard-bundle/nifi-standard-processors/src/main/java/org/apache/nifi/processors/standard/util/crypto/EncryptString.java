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

import org.apache.commons.codec.binary.Base64;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.EncryptContent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

//TODO: Refactor classname. Think of something non conflicting and meaningful.
/**
 * Encrypts string with a given encryptor
 */
public class EncryptString {

    /**
     * Performs decryption with given input string and encryptor.
     * The input must be of Base64 encoded string.
     * @param str Base64 encoded encrypted String
     * @param encryptor Encryptor which will be used for decryption
     * @return decrypted string of charset US-ASCII
     * @throws Exception
     */
    public static String performDecryption(String str, EncryptContent.Encryptor encryptor) throws Exception {
        //Initialize string and streams
        byte[] encryptedBytes = str.getBytes(StandardCharsets.US_ASCII);
        byte[] decodedBytes = Base64.decodeBase64(encryptedBytes);
        InputStream in = new ByteArrayInputStream(decodedBytes);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        String decryptedStr = null;

        try {
            encryptor.getDecryptionCallback().process(in,out);
            decryptedStr = new String(out.toByteArray(), StandardCharsets.US_ASCII);
        } catch (IOException e) {
            throw new ProcessException(e);
        } finally {
            in.close();
            out.close();
        }

        return decryptedStr;
    }

    /**
     * Performs encryption with given input string. The final encrypted string is
     * encoded to Base64 to prevent data loss
     * @param str String to be encrypted
     * @param encryptor Encryptor which will be used for encryption
     * @return Base64 encode string after performing encryption
     * @throws Exception
     */
    public static String performEncryption(String str, EncryptContent.Encryptor encryptor) throws Exception {
        String encodedEncryptedStr = null;
        InputStream in = new ByteArrayInputStream(str.getBytes(StandardCharsets.US_ASCII));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            encryptor.getEncryptionCallback().process(in, out);
            byte[] encryptedData = out.toByteArray();
            encodedEncryptedStr = Base64.encodeBase64String(encryptedData);
        } catch (IOException e) {
            throw new ProcessException(e);
        } finally {
            in.close();
            out.close();
        }
        return encodedEncryptedStr;
    }
}