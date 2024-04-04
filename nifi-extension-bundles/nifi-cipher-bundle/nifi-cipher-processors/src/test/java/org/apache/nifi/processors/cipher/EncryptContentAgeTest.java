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

import com.exceptionfactory.jagged.DecryptingChannelFactory;
import com.exceptionfactory.jagged.RecipientStanzaReader;
import com.exceptionfactory.jagged.framework.armor.ArmoredDecryptingChannelFactory;
import com.exceptionfactory.jagged.framework.stream.StandardDecryptingChannelFactory;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processors.cipher.age.AgePrivateKeyReader;
import org.apache.nifi.processors.cipher.age.AgeProviderResolver;
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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Provider;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EncryptContentAgeTest {

    private static final String BINARY_VERSION = "age-encryption.org";

    private static final String ASCII_HEADER = "-----BEGIN AGE ENCRYPTED FILE-----";

    private static final byte[] WORD = {'W', 'O', 'R', 'D'};

    private static final int BUFFER_CAPACITY = 20;

    private static final String PRIVATE_KEY_ENCODED = "AGE-SECRET-KEY-1NZKRS8L39Y2KVLXT7XX4DHFVDUUWN0699NJYR2EJS8RWGRYN279Q8GSFTN";

    private static final String PUBLIC_KEY_ENCODED = "age1qqnete0p2wzcc7y55trm3c4jzr608g4xdvyfw9jurugyt6maauussgn5gg";

    private static final String PUBLIC_KEY_CHECKSUM_INVALID = "age1qqnete0p2wzcc7y55trm3c4jzr608g4xdvyfw9jurugyt6maauussgn000";

    private static final String KEY_FILE_SUFFIX = ".key";

    private TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(EncryptContentAge.class);
    }

    @Test
    void testRequiredProperties() {
        runner.assertNotValid();

        runner.setProperty(EncryptContentAge.PUBLIC_KEY_RECIPIENTS, PUBLIC_KEY_ENCODED);

        runner.assertValid();
    }

    @Test
    void testPublicKeyNotValid() {
        runner.setProperty(EncryptContentAge.PUBLIC_KEY_RECIPIENTS, PRIVATE_KEY_ENCODED);

        runner.assertNotValid();
    }

    @Test
    void testVerifySuccessful() {
        runner.setProperty(EncryptContentAge.PUBLIC_KEY_RECIPIENTS, PUBLIC_KEY_ENCODED);
        runner.setProperty(EncryptContentAge.PUBLIC_KEY_SOURCE, KeySource.PROPERTIES);

        assertVerificationResultOutcomeEquals(ConfigVerificationResult.Outcome.SUCCESSFUL);
    }

    @Test
    void testVerifyFailedChecksumInvalid() {
        runner.setProperty(EncryptContentAge.PUBLIC_KEY_RECIPIENTS, PUBLIC_KEY_CHECKSUM_INVALID);
        runner.setProperty(EncryptContentAge.PUBLIC_KEY_SOURCE, KeySource.PROPERTIES);

        assertVerificationResultOutcomeEquals(ConfigVerificationResult.Outcome.FAILED);
    }

    @Test
    void testVerifyFailedResourcesNotFound() {
        runner.setProperty(EncryptContentAge.PUBLIC_KEY_SOURCE, KeySource.RESOURCES);

        assertVerificationResultOutcomeEquals(ConfigVerificationResult.Outcome.FAILED);
    }

    @Test
    void testRunSuccessAscii() throws GeneralSecurityException, IOException {
        runner.setProperty(EncryptContentAge.PUBLIC_KEY_RECIPIENTS, PUBLIC_KEY_ENCODED);
        runner.setProperty(EncryptContentAge.PUBLIC_KEY_SOURCE, KeySource.PROPERTIES);
        runner.setProperty(EncryptContentAge.FILE_ENCODING, FileEncoding.ASCII);

        runner.enqueue(WORD);
        runner.run();

        runner.assertAllFlowFilesTransferred(EncryptContentAge.SUCCESS);
        assertAsciiFound();
    }

    @Test
    void testRunSuccessBinary() throws GeneralSecurityException, IOException {
        runner.setProperty(EncryptContentAge.PUBLIC_KEY_RECIPIENTS, PUBLIC_KEY_ENCODED);
        runner.setProperty(EncryptContentAge.PUBLIC_KEY_SOURCE, KeySource.PROPERTIES);
        runner.setProperty(EncryptContentAge.FILE_ENCODING, FileEncoding.BINARY);

        runner.enqueue(WORD);
        runner.run();

        runner.assertAllFlowFilesTransferred(EncryptContentAge.SUCCESS);
        assertBinaryFound();
    }

    @Test
    void testRunSuccessBinaryKeySourceResources(@TempDir final Path tempDir) throws GeneralSecurityException, IOException {
        final Path tempFile = createTempFile(tempDir);
        Files.writeString(tempFile, PUBLIC_KEY_ENCODED);

        runner.setProperty(EncryptContentAge.PUBLIC_KEY_RECIPIENT_RESOURCES, tempFile.toString());
        runner.setProperty(EncryptContentAge.PUBLIC_KEY_SOURCE, KeySource.RESOURCES);
        runner.setProperty(EncryptContentAge.FILE_ENCODING, FileEncoding.BINARY);

        runner.enqueue(WORD);
        runner.run();

        runner.assertAllFlowFilesTransferred(EncryptContentAge.SUCCESS);
        assertBinaryFound();
    }

    @Test
    void testRunScheduledErrorPublicKeyNotFound(@TempDir final Path tempDir) throws IOException {
        final Path tempFile = createTempFile(tempDir);

        runner.setProperty(EncryptContentAge.PUBLIC_KEY_RECIPIENT_RESOURCES, tempFile.toString());
        runner.setProperty(EncryptContentAge.PUBLIC_KEY_SOURCE, KeySource.RESOURCES);

        runner.enqueue(WORD);

        assertThrows(AssertionFailedError.class, runner::run);
    }

    private void assertBinaryFound() throws GeneralSecurityException, IOException {
        final Iterator<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(EncryptContentAge.SUCCESS).iterator();

        assertTrue(flowFiles.hasNext());

        final MockFlowFile flowFile = flowFiles.next();
        final String content = flowFile.getContent();

        assertTrue(content.startsWith(BINARY_VERSION));

        final byte[] decrypted = getDecrypted(FileEncoding.BINARY, flowFile.getContentStream());
        assertArrayEquals(WORD, decrypted);
    }

    private void assertAsciiFound() throws GeneralSecurityException, IOException {
        final Iterator<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(EncryptContentAge.SUCCESS).iterator();

        assertTrue(flowFiles.hasNext());

        final MockFlowFile flowFile = flowFiles.next();
        final String content = flowFile.getContent();

        assertTrue(content.startsWith(ASCII_HEADER));

        final byte[] decrypted = getDecrypted(FileEncoding.ASCII, flowFile.getContentStream());
        assertArrayEquals(WORD, decrypted);
    }

    private void assertVerificationResultOutcomeEquals(final ConfigVerificationResult.Outcome expected) {
        final VerifiableProcessor processor = (VerifiableProcessor) runner.getProcessor();
        final Iterator<ConfigVerificationResult> results = processor.verify(runner.getProcessContext(), runner.getLogger(), Collections.emptyMap()).iterator();

        assertTrue(results.hasNext());
        final ConfigVerificationResult result = results.next();
        assertEquals(expected, result.getOutcome());
    }

    private byte[] getDecrypted(final FileEncoding fileEncoding, final InputStream inputStream) throws GeneralSecurityException, IOException {
        final DecryptingChannelFactory decryptingChannelFactory = getDecryptingChannelFactory(fileEncoding);
        final AgePrivateKeyReader privateKeyReader = new AgePrivateKeyReader();
        final ByteArrayInputStream encodedInputStream = new ByteArrayInputStream(PRIVATE_KEY_ENCODED.getBytes(StandardCharsets.UTF_8));
        final List<RecipientStanzaReader> recipientStanzaReaders = privateKeyReader.read(encodedInputStream);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final WritableByteChannel outputChannel = Channels.newChannel(outputStream);
        try (final ReadableByteChannel decryptingChannel = decryptingChannelFactory.newDecryptingChannel(Channels.newChannel(inputStream), recipientStanzaReaders)) {
            final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_CAPACITY);
            decryptingChannel.read(buffer);
            buffer.flip();
            outputChannel.write(buffer);
        }

        return outputStream.toByteArray();
    }

    private DecryptingChannelFactory getDecryptingChannelFactory(final FileEncoding fileEncoding) {
        final Provider provider = AgeProviderResolver.getCipherProvider();

        return switch (fileEncoding) {
            case ASCII -> new ArmoredDecryptingChannelFactory(provider);
            case BINARY -> new StandardDecryptingChannelFactory(provider);
        };
    }

    private Path createTempFile(final Path tempDir) throws IOException {
        return Files.createTempFile(tempDir, EncryptContentAgeTest.class.getSimpleName(), KEY_FILE_SUFFIX);
    }
}
