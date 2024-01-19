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
package org.apache.nifi.processors.cipher;

import com.exceptionfactory.jagged.EncryptingChannelFactory;
import com.exceptionfactory.jagged.RecipientStanzaWriter;
import com.exceptionfactory.jagged.framework.armor.ArmoredEncryptingChannelFactory;
import com.exceptionfactory.jagged.framework.stream.StandardEncryptingChannelFactory;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processors.cipher.age.AgeProviderResolver;
import org.apache.nifi.processors.cipher.age.AgePublicKeyReader;
import org.apache.nifi.processors.cipher.age.FileEncoding;
import org.apache.nifi.processors.cipher.age.KeySource;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opentest4j.AssertionFailedError;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Provider;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DecryptContentAgeTest {

    private static final byte[] WORD = {'W', 'O', 'R', 'D'};

    private static final String CONTENT_WORD = new String(WORD);

    private static final String PRIVATE_KEY_CHECKSUM_INVALID = "AGE-SECRET-KEY-1NZKRS8L39Y2KVLXT7XX4DHFVDUUWN0699NJYR2EJS8RWGRYN279Q8GSFFF";

    private static final String PRIVATE_KEY_ENCODED = "AGE-SECRET-KEY-1NZKRS8L39Y2KVLXT7XX4DHFVDUUWN0699NJYR2EJS8RWGRYN279Q8GSFTN";

    private static final String PRIVATE_KEY_SECOND = "AGE-SECRET-KEY-1AU0T8M9GWJ4PEQK9TGS54T6VHRL8DLFTZ7AWYJDFTLMDZZWZQKDSA8K882";

    private static final String PUBLIC_KEY_ENCODED = "age1qqnete0p2wzcc7y55trm3c4jzr608g4xdvyfw9jurugyt6maauussgn5gg";

    private static final String KEY_FILE_SUFFIX = ".key";

    private TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(DecryptContentAge.class);
    }

    @Test
    void testRequiredProperties() {
        runner.assertNotValid();

        runner.setProperty(DecryptContentAge.PRIVATE_KEY_IDENTITIES, PRIVATE_KEY_ENCODED);

        runner.assertValid();
    }

    @Test
    void testPrivateKeyNotValid() {
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_IDENTITIES, PUBLIC_KEY_ENCODED);

        runner.assertNotValid();
    }

    @Test
    void testVerifySuccessful() {
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_IDENTITIES, PRIVATE_KEY_ENCODED);
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_SOURCE, KeySource.PROPERTIES);

        assertVerificationResultOutcomeEquals(ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    void testVerifyFailedChecksumInvalid() {
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_IDENTITIES, PRIVATE_KEY_CHECKSUM_INVALID);
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_SOURCE, KeySource.PROPERTIES);

        assertVerificationResultOutcomeEquals(ConfigVerificationResult.Outcome.FAILED);
    }

    @Test
    void testVerifyFailedResourcesNotConfigured() {
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_SOURCE, KeySource.RESOURCES);

        assertVerificationResultOutcomeEquals(ConfigVerificationResult.Outcome.FAILED);
    }

    @Test
    void testRunSuccessAscii() throws GeneralSecurityException, IOException {
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_IDENTITIES, PRIVATE_KEY_ENCODED);
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_SOURCE, KeySource.PROPERTIES);

        assertSuccess(FileEncoding.ASCII);
    }

    @Test
    void testRunSuccessBinary() throws GeneralSecurityException, IOException {
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_IDENTITIES, PRIVATE_KEY_ENCODED);
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_SOURCE, KeySource.PROPERTIES);

        assertSuccess(FileEncoding.BINARY);
    }

    @Test
    void testRunSuccessBinaryMultiplePrivateKeys() throws GeneralSecurityException, IOException {
        final String multiplePrivateKeys = String.format("%s%n%s", PRIVATE_KEY_ENCODED, PRIVATE_KEY_SECOND);
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_IDENTITIES, multiplePrivateKeys);
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_SOURCE, KeySource.PROPERTIES);

        assertSuccess(FileEncoding.BINARY);
    }

    @Test
    void testRunSuccessBinaryKeySourceResources(@TempDir final Path tempDir) throws GeneralSecurityException, IOException {
        final Path tempFile = createTempFile(tempDir);
        Files.writeString(tempFile, PRIVATE_KEY_ENCODED);

        runner.setProperty(DecryptContentAge.PRIVATE_KEY_IDENTITY_RESOURCES, tempFile.toString());
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_SOURCE, KeySource.RESOURCES);

        assertSuccess(FileEncoding.BINARY);
    }

    @Test
    void testRunScheduledErrorPrivateKeyNotFound(@TempDir final Path tempDir) throws GeneralSecurityException, IOException {
        final Path tempFile = createTempFile(tempDir);

        runner.setProperty(DecryptContentAge.PRIVATE_KEY_IDENTITY_RESOURCES, tempFile.toString());
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_SOURCE, KeySource.RESOURCES);

        final byte[] encrypted = getEncrypted(FileEncoding.BINARY);
        runner.enqueue(encrypted);

        assertThrows(AssertionFailedError.class, runner::run);
    }

    @Test
    void testRunFailure() {
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_IDENTITIES, PRIVATE_KEY_ENCODED);
        runner.setProperty(DecryptContentAge.PRIVATE_KEY_SOURCE, KeySource.PROPERTIES);

        runner.enqueue(WORD);
        runner.run();

        runner.assertAllFlowFilesTransferred(DecryptContentAge.FAILURE);
    }

    private void assertSuccess(final FileEncoding fileEncoding) throws GeneralSecurityException, IOException {
        final byte[] encrypted = getEncrypted(fileEncoding);
        runner.enqueue(encrypted);
        runner.run();

        runner.assertAllFlowFilesTransferred(DecryptContentAge.SUCCESS);

        final Iterator<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(DecryptContentAge.SUCCESS).iterator();
        assertTrue(flowFiles.hasNext());
        final MockFlowFile flowFile = flowFiles.next();

        final String content = flowFile.getContent();
        assertEquals(CONTENT_WORD, content);
    }

    private void assertVerificationResultOutcomeEquals(final ConfigVerificationResult.Outcome expected) {
        final VerifiableProcessor processor = (VerifiableProcessor) runner.getProcessor();
        final Iterator<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap()).iterator();

        assertTrue(results.hasNext());
        final ConfigVerificationResult result = results.next();
        assertEquals(expected, result.getOutcome());
    }

    private byte[] getEncrypted(final FileEncoding fileEncoding) throws GeneralSecurityException, IOException {
        final EncryptingChannelFactory encryptingChannelFactory = getEncryptingChannelFactory(fileEncoding);
        final AgePublicKeyReader publicKeyReader = new AgePublicKeyReader();
        final ByteArrayInputStream encodedInputStream = new ByteArrayInputStream(PUBLIC_KEY_ENCODED.getBytes(StandardCharsets.UTF_8));
        final List<RecipientStanzaWriter> recipientStanzaWriters = publicKeyReader.read(encodedInputStream);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final WritableByteChannel outputChannel = Channels.newChannel(outputStream);
        try (final WritableByteChannel encryptingChannel = encryptingChannelFactory.newEncryptingChannel(outputChannel, recipientStanzaWriters)) {
            final ByteBuffer buffer = ByteBuffer.wrap(WORD);
            encryptingChannel.write(buffer);
        }

        return outputStream.toByteArray();
    }

    private EncryptingChannelFactory getEncryptingChannelFactory(final FileEncoding fileEncoding) {
        final Provider provider = AgeProviderResolver.getCipherProvider();

        return switch (fileEncoding) {
            case ASCII -> new ArmoredEncryptingChannelFactory(provider);
            case BINARY -> new StandardEncryptingChannelFactory(provider);
        };
    }

    private Path createTempFile(final Path tempDir) throws IOException {
        return Files.createTempFile(tempDir, DecryptContentAgeTest.class.getSimpleName(), KEY_FILE_SUFFIX);
    }
}
