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
package org.apache.nifi.processors.pgp;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.pgp.service.api.PGPPublicKeyService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.pgp.attributes.CompressionAlgorithm;
import org.apache.nifi.processors.pgp.attributes.FileEncoding;
import org.apache.nifi.processors.pgp.attributes.SymmetricKeyAlgorithm;
import org.apache.nifi.processors.pgp.exception.PGPEncryptionException;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StringUtils;

import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.operator.PGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.PGPKeyEncryptionMethodGenerator;
import org.bouncycastle.openpgp.operator.bc.BcPGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.bc.BcPublicKeyKeyEncryptionMethodGenerator;
import org.bouncycastle.openpgp.operator.jcajce.JcePBEKeyEncryptionMethodGenerator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Encrypt Content using Open Pretty Good Privacy encryption methods
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"PGP", "GPG", "OpenPGP", "Encryption", "RFC 4880"})
@CapabilityDescription("Encrypt Contents using OpenPGP")
@SeeAlso(DecryptContentPGP.class)
@WritesAttributes({
        @WritesAttribute(attribute = PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM, description = "Symmetric-Key Algorithm"),
        @WritesAttribute(attribute = PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_BLOCK_CIPHER, description = "Symmetric-Key Algorithm Block Cipher"),
        @WritesAttribute(attribute = PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_KEY_SIZE, description = "Symmetric-Key Algorithm Key Size"),
        @WritesAttribute(attribute = PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_ID, description = "Symmetric-Key Algorithm Identifier"),
        @WritesAttribute(attribute = PGPAttributeKey.FILE_ENCODING, description = "File Encoding"),
        @WritesAttribute(attribute = PGPAttributeKey.COMPRESS_ALGORITHM, description = "Compression Algorithm"),
        @WritesAttribute(attribute = PGPAttributeKey.COMPRESS_ALGORITHM_ID, description = "Compression Algorithm Identifier"),
})
public class EncryptContentPGP extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Encryption Succeeded")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Encryption Failed")
            .build();

    public static final PropertyDescriptor SYMMETRIC_KEY_ALGORITHM = new PropertyDescriptor.Builder()
            .name("symmetric-key-algorithm")
            .displayName("Symmetric-Key Algorithm")
            .description("Symmetric-Key Algorithm for encryption")
            .required(true)
            .defaultValue(SymmetricKeyAlgorithm.AES_256.toString())
            .allowableValues(SymmetricKeyAlgorithm.values())
            .build();

    public static final PropertyDescriptor COMPRESSION_ALGORITHM = new PropertyDescriptor.Builder()
            .name("compression-algorithm")
            .displayName("Compression Algorithm")
            .description("Compression Algorithm for encryption")
            .required(true)
            .defaultValue(CompressionAlgorithm.ZIP.toString())
            .allowableValues(CompressionAlgorithm.values())
            .build();

    public static final PropertyDescriptor FILE_ENCODING = new PropertyDescriptor.Builder()
            .name("file-encoding")
            .displayName("File Encoding")
            .description("File Encoding for encryption")
            .required(true)
            .defaultValue(FileEncoding.BINARY.toString())
            .allowableValues(FileEncoding.values())
            .build();

    public static final PropertyDescriptor PASSPHRASE = new PropertyDescriptor.Builder()
            .name("passphrase")
            .displayName("Passphrase")
            .description("Passphrase used for encrypting data with Password-Based Encryption")
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PUBLIC_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("public-key-service")
            .displayName("Public Key Service")
            .description("PGP Public Key Service for encrypting data with Public Key Encryption")
            .identifiesControllerService(PGPPublicKeyService.class)
            .build();

    public static final PropertyDescriptor PUBLIC_KEY_SEARCH = new PropertyDescriptor.Builder()
            .name("public-key-search")
            .displayName("Public Key Search")
            .description("PGP Public Key Search will be used to match against the User ID or Key ID when formatted as uppercase hexadecimal string of 16 characters")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .dependsOn(PUBLIC_KEY_SERVICE)
            .build();

    /** Enable Integrity Protection as described in RFC 4880 Section 5.13 */
    private static final boolean ENCRYPTION_INTEGRITY_PACKET_ENABLED = true;

    private static final int OUTPUT_BUFFER_SIZE = 8192;

    private static final Set<Relationship> RELATIONSHIPS = new HashSet<>(Arrays.asList(SUCCESS, FAILURE));

    private static final List<PropertyDescriptor> DESCRIPTORS = Arrays.asList(
            SYMMETRIC_KEY_ALGORITHM,
            COMPRESSION_ALGORITHM,
            FILE_ENCODING,
            PASSPHRASE,
            PUBLIC_KEY_SERVICE,
            PUBLIC_KEY_SEARCH
    );

    /**
     * Get Relationships
     *
     * @return Processor Relationships
     */
    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    /**
     * Get Supported Property Descriptors
     *
     * @return Processor Supported Property Descriptors
     */
    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    /**
     * On Trigger encrypts Flow File contents using configured properties
     *
     * @param context Process Context
     * @param session Process Session
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        try {
            final SymmetricKeyAlgorithm symmetricKeyAlgorithm = getSymmetricKeyAlgorithm(context);
            final FileEncoding fileEncoding = getFileEncoding(context);
            final CompressionAlgorithm compressionAlgorithm = getCompressionAlgorithm(context);
            final StreamCallback callback = getEncryptStreamCallback(context, flowFile, symmetricKeyAlgorithm, compressionAlgorithm, fileEncoding);
            flowFile = session.write(flowFile, callback);

            final Map<String, String> attributes = getAttributes(symmetricKeyAlgorithm, fileEncoding, compressionAlgorithm);
            flowFile = session.putAllAttributes(flowFile, attributes);

            session.transfer(flowFile, SUCCESS);
        } catch (final RuntimeException e) {
            getLogger().error("Encryption Failed {}", flowFile, e);
            session.transfer(flowFile, FAILURE);
        }
    }

    /**
     * Custom Validate requires at least one encryption property to be configured
     *
     * @param context Validation Context
     * @return Collection of Validation Results
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final String passphrase = context.getProperty(PASSPHRASE).getValue();
        if (StringUtils.isBlank(passphrase)) {
            final PGPPublicKeyService publicKeyService = context.getProperty(PUBLIC_KEY_SERVICE).asControllerService(PGPPublicKeyService.class);
            if (publicKeyService == null) {
                final String explanation = String.format("Neither [%s] nor [%s] configured", PASSPHRASE.getDisplayName(), PUBLIC_KEY_SERVICE.getDisplayName());
                final ValidationResult result = new ValidationResult.Builder()
                        .valid(false)
                        .subject(getClass().getSimpleName())
                        .explanation(explanation)
                        .build();
                results.add(result);
            }
        }

        if (context.getProperty(PUBLIC_KEY_SERVICE).isSet()) {
            final ValidationResult result = new ValidationResult.Builder()
                    .valid(context.getProperty(PUBLIC_KEY_SEARCH).isSet())
                    .subject(PUBLIC_KEY_SERVICE.getDisplayName())
                    .explanation(String.format("[%s] requires [%s]", PUBLIC_KEY_SERVICE.getDisplayName(), PUBLIC_KEY_SEARCH.getDisplayName()))
                    .build();
            results.add(result);
        }

        if (context.getProperty(PUBLIC_KEY_SEARCH).isSet()) {
            final ValidationResult result = new ValidationResult.Builder()
                    .valid(context.getProperty(PUBLIC_KEY_SERVICE).isSet())
                    .subject(PUBLIC_KEY_SERVICE.getDisplayName())
                    .explanation(String.format("[%s] requires [%s]", PUBLIC_KEY_SEARCH.getDisplayName(), PUBLIC_KEY_SERVICE.getDisplayName()))
                    .build();
            results.add(result);
        }

        return results;
    }

    private StreamCallback getEncryptStreamCallback(final ProcessContext context, final FlowFile flowFile,
                                                    final SymmetricKeyAlgorithm symmetricKeyAlgorithm,
                                                    final CompressionAlgorithm compressionAlgorithm,
                                                    final FileEncoding fileEncoding) {
        final SecureRandom secureRandom = new SecureRandom();
        final PGPDataEncryptorBuilder dataEncryptorBuilder = new BcPGPDataEncryptorBuilder(symmetricKeyAlgorithm.getId())
                .setSecureRandom(secureRandom)
                .setWithIntegrityPacket(ENCRYPTION_INTEGRITY_PACKET_ENABLED);
        final PGPEncryptedDataGenerator encryptedDataGenerator = new PGPEncryptedDataGenerator(dataEncryptorBuilder);
        final List<PGPKeyEncryptionMethodGenerator> methodGenerators = getEncryptionMethodGenerators(context, flowFile, secureRandom);
        methodGenerators.forEach(encryptedDataGenerator::addMethod);

        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        return new EncryptStreamCallback(filename, fileEncoding, encryptedDataGenerator, compressionAlgorithm);
    }

    private List<PGPKeyEncryptionMethodGenerator> getEncryptionMethodGenerators(final ProcessContext context,
                                                                                final FlowFile flowFile,
                                                                                final SecureRandom secureRandom) {
        final List<PGPKeyEncryptionMethodGenerator> generators = new ArrayList<>();

        final PropertyValue passphraseProperty = context.getProperty(PASSPHRASE);
        if (passphraseProperty.isSet()) {
            final char[] passphrase = passphraseProperty.getValue().toCharArray();
            generators.add(new JcePBEKeyEncryptionMethodGenerator(passphrase).setSecureRandom(secureRandom));
        }

        final String publicKeySearch = context.getProperty(PUBLIC_KEY_SEARCH).evaluateAttributeExpressions(flowFile).getValue();
        if (StringUtils.isNotBlank(publicKeySearch)) {
            getLogger().debug("Public Key Search [{}]", publicKeySearch);

            final PGPPublicKeyService publicKeyService = context.getProperty(PUBLIC_KEY_SERVICE).asControllerService(PGPPublicKeyService.class);
            final Optional<PGPPublicKey> optionalPublicKey = publicKeyService.findPublicKey(publicKeySearch);
            if (optionalPublicKey.isPresent()) {
                final PGPPublicKey publicKey = optionalPublicKey.get();
                generators.add(new BcPublicKeyKeyEncryptionMethodGenerator(publicKey).setSecureRandom(secureRandom));
            } else {
                throw new PGPEncryptionException(String.format("Public Key not found using search [%s]", publicKeySearch));
            }
        }

        return generators;
    }

    private SymmetricKeyAlgorithm getSymmetricKeyAlgorithm(final ProcessContext context) {
        final String algorithm = context.getProperty(SYMMETRIC_KEY_ALGORITHM).getValue();
        return SymmetricKeyAlgorithm.valueOf(algorithm);
    }

    private CompressionAlgorithm getCompressionAlgorithm(final ProcessContext context) {
        final String algorithm = context.getProperty(COMPRESSION_ALGORITHM).getValue();
        return CompressionAlgorithm.valueOf(algorithm);
    }

    private FileEncoding getFileEncoding(final ProcessContext context) {
        final String encoding = context.getProperty(FILE_ENCODING).getValue();
        return FileEncoding.valueOf(encoding);
    }

    private Map<String, String> getAttributes(final SymmetricKeyAlgorithm symmetricKeyAlgorithm,
                                              final FileEncoding fileEncoding,
                                              final CompressionAlgorithm compressionAlgorithm) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM, symmetricKeyAlgorithm.toString());
        attributes.put(PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_BLOCK_CIPHER, symmetricKeyAlgorithm.getBlockCipher().toString());
        attributes.put(PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_KEY_SIZE, Integer.toString(symmetricKeyAlgorithm.getKeySize()));
        attributes.put(PGPAttributeKey.SYMMETRIC_KEY_ALGORITHM_ID, Integer.toString(symmetricKeyAlgorithm.getId()));
        attributes.put(PGPAttributeKey.FILE_ENCODING, fileEncoding.toString());
        attributes.put(PGPAttributeKey.COMPRESS_ALGORITHM, compressionAlgorithm.toString());
        attributes.put(PGPAttributeKey.COMPRESS_ALGORITHM_ID, Integer.toString(compressionAlgorithm.getId()));
        return attributes;
    }

    private static class EncryptStreamCallback implements StreamCallback {
        private static final char DATA_FORMAT = PGPLiteralData.BINARY;

        private final String filename;

        private final FileEncoding fileEncoding;

        private final PGPEncryptedDataGenerator encryptedDataGenerator;

        private final CompressionAlgorithm compressionAlgorithm;

        public EncryptStreamCallback(final String filename, final FileEncoding fileEncoding, final PGPEncryptedDataGenerator encryptedDataGenerator, final CompressionAlgorithm compressionAlgorithm) {
            this.filename = filename;
            this.fileEncoding = fileEncoding;
            this.encryptedDataGenerator = encryptedDataGenerator;
            this.compressionAlgorithm = compressionAlgorithm;
        }

        /**
         * Process Input Stream and write encrypted contents to Output Stream
         *
         * @param inputStream  Input Stream
         * @param outputStream Output Stream for encrypted contents
         * @throws IOException Thrown when unable to read or write streams
         */
        @Override
        public void process(final InputStream inputStream, final OutputStream outputStream) throws IOException {
            try (final OutputStream encodingOutputStream = getEncodingOutputStream(outputStream)) {
                processEncoding(inputStream, encodingOutputStream);
            } catch (final PGPException e) {
                throw new PGPEncryptionException("PGP Encryption Stream Processing Failed", e);
            }
        }

        private OutputStream getEncodingOutputStream(final OutputStream outputStream) throws PGPException {
            OutputStream encodingOutputStream = outputStream;
            if (FileEncoding.ASCII.equals(fileEncoding)) {
                encodingOutputStream = new ArmoredOutputStream(outputStream);
            }
            return encodingOutputStream;
        }

        private void processEncoding(final InputStream inputStream, final OutputStream encodingOutputStream) throws IOException, PGPException {
            try (final OutputStream encryptedOutputStream = encryptedDataGenerator.open(encodingOutputStream, createBuffer())) {
                final PGPCompressedDataGenerator compressedDataGenerator = new PGPCompressedDataGenerator(compressionAlgorithm.getId());
                try (final OutputStream compressedOutputStream = compressedDataGenerator.open(encryptedOutputStream, createBuffer())) {
                    final PGPLiteralDataGenerator literalDataGenerator = new PGPLiteralDataGenerator();
                    try (final OutputStream literalOutputStream = literalDataGenerator.open(compressedOutputStream, DATA_FORMAT, filename, new Date(), createBuffer())) {
                        StreamUtils.copy(inputStream, literalOutputStream);
                    }
                    literalDataGenerator.close();
                }
                compressedDataGenerator.close();
            }
            encryptedDataGenerator.close();
        }

        private byte[] createBuffer() {
            return new byte[OUTPUT_BUFFER_SIZE];
        }
    }
}
