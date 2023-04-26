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

import org.apache.nifi.encrypt.PropertyEncryptionMethod;
import org.apache.nifi.encrypt.PropertyEncryptor;
import org.apache.nifi.encrypt.PropertyEncryptorBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardFlowEncryptorTest {
    private static final String INPUT_KEY = UUID.randomUUID().toString();

    private static final String OUTPUT_KEY = UUID.randomUUID().toString();

    private static final String ENCRYPTED_FORMAT = "enc{%s}";

    private static final String PATTERN_REGEX = "enc\\{([^}]+?)}";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_REGEX);

    private PropertyEncryptor inputEncryptor;

    private PropertyEncryptor outputEncryptor;

    private StandardFlowEncryptor flowEncryptor;


    @BeforeEach
    public void setEncryptors() {
        inputEncryptor = getPropertyEncryptor(INPUT_KEY, PropertyEncryptionMethod.NIFI_PBKDF2_AES_GCM_256.name());
        outputEncryptor = getPropertyEncryptor(OUTPUT_KEY, PropertyEncryptionMethod.NIFI_ARGON2_AES_GCM_256.name());
        flowEncryptor = new StandardFlowEncryptor();
    }

    @Test
    public void testProcessEncrypted() {
        final String property = StandardFlowEncryptorTest.class.getSimpleName();
        final String encryptedProperty = String.format(ENCRYPTED_FORMAT, inputEncryptor.encrypt(property));
        final String encryptedRow = String.format("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>%n" +
                "<test>%s</test>", encryptedProperty);

        final InputStream inputStream = new ByteArrayInputStream(encryptedRow.getBytes(StandardCharsets.UTF_8));
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        flowEncryptor.processFlow(inputStream, outputStream, inputEncryptor, outputEncryptor);

        final String outputEncrypted = outputStream.toString();
        final Matcher matcher = PATTERN.matcher(outputEncrypted);
        assertTrue(matcher.find(), String.format("Encrypted Pattern not found [%s]", outputEncrypted));

        final String outputEncryptedProperty = matcher.group(1);
        final String outputDecrypted = outputEncryptor.decrypt(outputEncryptedProperty);
        assertEquals(property, outputDecrypted);
    }

    @Test
    public void testProcessNoEncrypted() {
        final String property = String.format("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>%n" +
                "<test>%s</test>", StandardFlowEncryptorTest.class.getSimpleName());

        final InputStream inputStream = new ByteArrayInputStream(property.getBytes(StandardCharsets.UTF_8));
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        flowEncryptor.processFlow(inputStream, outputStream, inputEncryptor, outputEncryptor);

        final String outputProperty = outputStream.toString();
        assertEquals(removeXmlDeclaration(property).trim(), removeXmlDeclaration(outputProperty).trim());
    }

    @Test
    public void testProcessJson() throws IOException {
        final String password = StandardFlowEncryptorTest.class.getSimpleName();
        final String encryptedPassword = String.format(ENCRYPTED_FORMAT, inputEncryptor.encrypt(password));

        final String sampleFlowJson = getSampleFlowJson(encryptedPassword);

        try (final InputStream inputStream = new ByteArrayInputStream(sampleFlowJson.getBytes(StandardCharsets.UTF_8))) {
            try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                flowEncryptor.processFlow(inputStream, outputStream, inputEncryptor, outputEncryptor);

                final String outputFlowJson = outputStream.toString();

                compareFlow(sampleFlowJson.trim(), outputFlowJson.trim());
            }
        }
    }

    @Test
    public void testProcessXml() throws IOException {
        final String password = StandardFlowEncryptorTest.class.getSimpleName();
        final String encryptedPassword = String.format(ENCRYPTED_FORMAT, inputEncryptor.encrypt(password));
        final String sampleFlowXml = getSampleFlowXml(encryptedPassword);
        try (final InputStream inputStream = new ByteArrayInputStream(sampleFlowXml.getBytes(StandardCharsets.UTF_8))) {
            try (final ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                flowEncryptor.processFlow(inputStream, outputStream, inputEncryptor, outputEncryptor);
                final String outputXml = outputStream.toString();

                compareFlow(removeXmlDeclaration(sampleFlowXml).trim(), removeXmlDeclaration(outputXml).trim());
            }
        }
    }

    private PropertyEncryptor getPropertyEncryptor(final String propertiesKey, final String propertiesAlgorithm) {
        return new PropertyEncryptorBuilder(propertiesKey).setAlgorithm(propertiesAlgorithm).build();
    }

    private void compareFlow(final String sampleFlow, final String outputFlow) {
        final Matcher inputMatcher = PATTERN.matcher(sampleFlow);
        final Matcher outputMatcher = PATTERN.matcher(outputFlow);
        assertTrue(inputMatcher.find() && outputMatcher.find());
        assertEquals(inputEncryptor.decrypt(inputMatcher.group(1)), outputEncryptor.decrypt(outputMatcher.group(1)));

        assertEquals(sampleFlow.replaceAll(PATTERN_REGEX, ""), outputFlow.replaceAll(PATTERN_REGEX, ""));
    }

    private String getSampleFlowJson(final String password) {
        Objects.requireNonNull(password);
        return String.format("{\"properties\":{\"username\":\"sample_username\",\"password\":\"%s\",\"position\":1.123456789123456789}}", password);
    }

    private String getSampleFlowXml(final String password) {
        Objects.requireNonNull(password);
        final String flowXml = String.format("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>%n" +
                "<processor>%n" +
                "\t<property>%n" +
                "\t\t<name>Username</name>%n" +
                "\t\t<value>SAMPLE_USERNAME</value>%n" +
                "\t</property>%n" +
                "\t<property>%n" +
                "\t\t<name>Password</name>%n" +
                "\t\t<value>%s</value>%n" +
                "\t</property>%n" +
                "</processor>", password);

        return getProcessedFlowXml(flowXml);
    }

    private String getProcessedFlowXml(final String flowXml) {
        final PropertyEncryptor encryptor = new PropertyEncryptor() {
            @Override
            public String encrypt(String property) {
                return property;
            }

            @Override
            public String decrypt(String encryptedProperty) {
                return encryptedProperty;
            }
        };
        final InputStream inputStream = new ByteArrayInputStream(flowXml.getBytes(StandardCharsets.UTF_8));
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        flowEncryptor.processFlow(inputStream, outputStream, encryptor, encryptor);
        return outputStream.toString();
    }

    private String removeXmlDeclaration(final String xmlFlow) {
        return xmlFlow.replaceAll("<\\?xml.+\\?>", "");
    }
}
