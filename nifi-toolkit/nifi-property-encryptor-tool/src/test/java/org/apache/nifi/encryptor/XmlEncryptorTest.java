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
package org.apache.nifi.encryptor;

import org.apache.nifi.util.crypto.CryptographyUtils;
import org.apache.nifi.util.file.FileUtilities;
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.properties.scheme.StandardProtectionScheme;
import org.apache.nifi.xml.XmlDecryptor;
import org.apache.nifi.xml.XmlEncryptor;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class XmlEncryptorTest {

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210";
    private static final String KEY_HEX_256 = KEY_HEX_128 + KEY_HEX_128;
    static final ProtectionScheme DEFAULT_PROTECTION_SCHEME = new StandardProtectionScheme("aes/gcm");
    public static final String KEY_HEX = CryptographyUtils.isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128;
    private static final String PASSWORD = "thisIsABadPassword";

    @Test
    void testDecryptLoginIdentityProviders() throws URISyntaxException, IOException {
        String encryptedXmlFilename = "/login-identity-providers-populated-encrypted.xml";
        XmlDecryptor decryptor = intializeXmlDecryptor();
        File encryptedXmlFile = new File(XmlEncryptorTest.class.getResource(encryptedXmlFilename).toURI());
        File temporaryOutputFile = FileUtilities.getTemporaryOutputFile("decrypted", encryptedXmlFile);

        try (InputStream inputStream = new FileInputStream(encryptedXmlFile);
             FileOutputStream outputStream = new FileOutputStream(temporaryOutputFile)) {
             decryptor.decrypt(inputStream, outputStream);
        }

        assertEquals(3, verifyFileContains(temporaryOutputFile, PASSWORD));
    }

    @Test
    void testEncryptLoginIdentityProviders() throws URISyntaxException, IOException {
        String unencryptedXmlFilename = "/login-identity-providers-populated-unecrypted.xml";
        XmlEncryptor encryptor = intializeXmlEncryptor();
        File unencryptedXmlFile = new File(XmlEncryptorTest.class.getResource(unencryptedXmlFilename).toURI());
        File temporaryOutputFile = FileUtilities.getTemporaryOutputFile("encrypted", unencryptedXmlFile);

        try (InputStream inputStream = new FileInputStream(unencryptedXmlFile);
             FileOutputStream outputStream = new FileOutputStream(temporaryOutputFile)) {
             encryptor.encrypt(inputStream, outputStream);
        }

        assertEquals(2, verifyFileContains(temporaryOutputFile, DEFAULT_PROTECTION_SCHEME.getPath()));
    }

    @Test
    void testEncryptAuthorizers() throws URISyntaxException, IOException {
        String unencryptedXmlFilename = "/authorizers-populated-unencrypted.xml";
        XmlEncryptor encryptor = intializeXmlEncryptor();
        File unencryptedXmlFile = new File(XmlEncryptorTest.class.getResource(unencryptedXmlFilename).toURI());
        File temporaryOutputFile = FileUtilities.getTemporaryOutputFile("encrypted", unencryptedXmlFile);

        try (InputStream inputStream = new FileInputStream(unencryptedXmlFile);
             FileOutputStream outputStream = new FileOutputStream(temporaryOutputFile)) {
             encryptor.encrypt(inputStream, outputStream);
        }

        assertEquals(3, verifyFileContains(temporaryOutputFile, DEFAULT_PROTECTION_SCHEME.getPath()));
    }

    private XmlEncryptor intializeXmlEncryptor() {
        return new XmlEncryptor(StandardSensitivePropertyProviderFactory.withKey(KEY_HEX), DEFAULT_PROTECTION_SCHEME);
    }

    private XmlDecryptor intializeXmlDecryptor() {
        return new XmlDecryptor(StandardSensitivePropertyProviderFactory.withKey(KEY_HEX), DEFAULT_PROTECTION_SCHEME);
    }

    private int verifyFileContains(final File fileToCheck, final String fileContent) throws IOException {
        int count = 0;

        List<String> lines = Files.readAllLines(fileToCheck.toPath());
        for (String line : lines) {
            if (line.contains(fileContent)) {
                count++;
            }
        }
        return count;
    }
}