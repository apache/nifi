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
package org.apache.nifi.flow.encryptor;

import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorBuilder;
import org.apache.nifi.security.util.EncryptionMethod;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StandardFlowEncryptorTest {

    private static final String INPUT_KEY = UUID.randomUUID().toString();

    private static final String OUTPUT_KEY = UUID.randomUUID().toString();

    private static final String ENCRYPTED_FORMAT = "enc{%s}";

    private static final Pattern OUTPUT_PATTERN = Pattern.compile("^enc\\{([^}]+?)}$");

    private PropertyEncryptor inputEncryptor;

    private PropertyEncryptor outputEncryptor;

    private StandardFlowEncryptor flowEncryptor;

    @Before
    public void setEncryptors() {
        inputEncryptor = getPropertyEncryptor(INPUT_KEY, EncryptionMethod.MD5_256AES.getAlgorithm());
        outputEncryptor = getPropertyEncryptor(OUTPUT_KEY, EncryptionMethod.SHA256_256AES.getAlgorithm());
        flowEncryptor = new StandardFlowEncryptor();
    }

    @Test
    public void testProcessEncrypted() {
        final String property = StandardFlowEncryptorTest.class.getSimpleName();
        final String encryptedProperty = String.format(ENCRYPTED_FORMAT, inputEncryptor.encrypt(property));
        final String encryptedRow = String.format("%s%n", encryptedProperty);

        final InputStream inputStream = new ByteArrayInputStream(encryptedRow.getBytes(StandardCharsets.UTF_8));
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        flowEncryptor.processFlow(inputStream, outputStream, inputEncryptor, outputEncryptor);

        final String outputEncrypted = new String(outputStream.toByteArray());
        final Matcher matcher = OUTPUT_PATTERN.matcher(outputEncrypted);
        assertTrue(String.format("Encrypted Pattern not found [%s]", outputEncrypted), matcher.find());

        final String outputEncryptedProperty = matcher.group(1);
        final String outputDecrypted = outputEncryptor.decrypt(outputEncryptedProperty);
        assertEquals(property, outputDecrypted);
    }

    @Test
    public void testProcessNoEncrypted() {
        final String property = String.format("%s%n", StandardFlowEncryptorTest.class.getSimpleName());

        final InputStream inputStream = new ByteArrayInputStream(property.getBytes(StandardCharsets.UTF_8));
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        flowEncryptor.processFlow(inputStream, outputStream, inputEncryptor, outputEncryptor);

        final String outputProperty = new String(outputStream.toByteArray());
        assertEquals(property, outputProperty);
    }

    private PropertyEncryptor getPropertyEncryptor(final String propertiesKey, final String propertiesAlgorithm) {
        return new PropertyEncryptorBuilder(propertiesKey).setAlgorithm(propertiesAlgorithm).build();
    }
}
