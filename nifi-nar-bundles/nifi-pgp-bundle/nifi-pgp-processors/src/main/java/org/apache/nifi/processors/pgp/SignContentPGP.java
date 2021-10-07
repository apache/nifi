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
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.pgp.service.api.PGPPrivateKeyService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.pgp.attributes.CompressionAlgorithm;
import org.apache.nifi.processors.pgp.attributes.FileEncoding;
import org.apache.nifi.processors.pgp.attributes.HashAlgorithm;
import org.apache.nifi.processors.pgp.attributes.SigningStrategy;
import org.apache.nifi.processors.pgp.exception.PGPProcessException;
import org.apache.nifi.processors.pgp.io.EncodingStreamCallback;
import org.apache.nifi.processors.pgp.io.KeyIdentifierConverter;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPOnePassSignature;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureGenerator;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentSignerBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Sign Content using Open Pretty Good Privacy Private Keys
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"PGP", "GPG", "OpenPGP", "Encryption", "Signing", "RFC 4880"})
@CapabilityDescription("Sign content using OpenPGP Private Keys")
@SeeAlso({DecryptContentPGP.class, EncryptContentPGP.class, VerifyContentPGP.class})
@WritesAttributes({
        @WritesAttribute(attribute = PGPAttributeKey.COMPRESS_ALGORITHM, description = "Compression Algorithm"),
        @WritesAttribute(attribute = PGPAttributeKey.COMPRESS_ALGORITHM_ID, description = "Compression Algorithm Identifier"),
        @WritesAttribute(attribute = PGPAttributeKey.FILE_ENCODING, description = "File Encoding"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_ALGORITHM, description = "Signature Algorithm including key and hash algorithm names"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_HASH_ALGORITHM_ID, description = "Signature Hash Algorithm Identifier"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_KEY_ALGORITHM_ID, description = "Signature Key Algorithm Identifier"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_KEY_ID, description = "Signature Public Key Identifier"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_TYPE_ID, description = "Signature Type Identifier"),
        @WritesAttribute(attribute = PGPAttributeKey.SIGNATURE_VERSION, description = "Signature Version Number"),
})
public class SignContentPGP extends AbstractProcessor {

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Content signing succeeded")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Content signing failed")
            .build();

    public static final PropertyDescriptor COMPRESSION_ALGORITHM = new PropertyDescriptor.Builder()
            .name("compression-algorithm")
            .displayName("Compression Algorithm")
            .description("Compression Algorithm for signing")
            .required(true)
            .defaultValue(CompressionAlgorithm.ZIP.toString())
            .allowableValues(CompressionAlgorithm.values())
            .build();

    public static final PropertyDescriptor FILE_ENCODING = new PropertyDescriptor.Builder()
            .name("file-encoding")
            .displayName("File Encoding")
            .description("File Encoding for signing")
            .required(true)
            .defaultValue(FileEncoding.BINARY.toString())
            .allowableValues(FileEncoding.values())
            .build();

    public static final PropertyDescriptor HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("hash-algorithm")
            .displayName("Hash Algorithm")
            .description("Hash Algorithm for signing")
            .required(true)
            .defaultValue(HashAlgorithm.SHA512.toString())
            .allowableValues(HashAlgorithm.values())
            .build();

    public static final PropertyDescriptor SIGNING_STRATEGY = new PropertyDescriptor.Builder()
            .name("signing-strategy")
            .displayName("Signing Strategy")
            .description("Strategy for writing files to success after signing")
            .required(true)
            .defaultValue(SigningStrategy.SIGNED.toString())
            .allowableValues(SigningStrategy.values())
            .build();

    public static final PropertyDescriptor PRIVATE_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("private-key-service")
            .displayName("Private Key Service")
            .description("PGP Private Key Service for generating content signatures")
            .identifiesControllerService(PGPPrivateKeyService.class)
            .required(true)
            .build();

    public static final PropertyDescriptor PRIVATE_KEY_ID = new PropertyDescriptor.Builder()
            .name("private-key-id")
            .displayName("Private Key ID")
            .description("PGP Private Key Identifier formatted as uppercase hexadecimal string of 16 characters used for signing")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .required(true)
            .build();

    private static final Set<Relationship> RELATIONSHIPS = new HashSet<>(Arrays.asList(SUCCESS, FAILURE));

    private static final List<PropertyDescriptor> DESCRIPTORS = Arrays.asList(
            COMPRESSION_ALGORITHM,
            FILE_ENCODING,
            HASH_ALGORITHM,
            SIGNING_STRATEGY,
            PRIVATE_KEY_SERVICE,
            PRIVATE_KEY_ID
    );

    private static final boolean NESTED_SIGNATURE_DISABLED = false;

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
     * On Trigger generates signatures for Flow File contents using private keys
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
            final SignStreamCallback callback = getSignStreamCallback(context, flowFile);
            flowFile = session.write(flowFile, callback);
            flowFile = session.putAllAttributes(flowFile, callback.attributes);
            session.transfer(flowFile, SUCCESS);
        } catch (final RuntimeException e) {
            getLogger().error("Signing Failed {}", flowFile, e);
            session.transfer(flowFile, FAILURE);
        }
    }

    private SignStreamCallback getSignStreamCallback(final ProcessContext context, final FlowFile flowFile) {
        final FileEncoding fileEncoding = getFileEncoding(context);
        final CompressionAlgorithm compressionAlgorithm = getCompressionAlgorithm(context);
        final HashAlgorithm hashAlgorithm = getHashAlgorithm(context);
        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
        final SigningStrategy signingStrategy = getSigningStrategy(context);
        final PGPPrivateKey privateKey = getPrivateKey(context, flowFile);
        return new SignStreamCallback(fileEncoding, compressionAlgorithm, filename, hashAlgorithm, signingStrategy, privateKey);
    }

    private PGPPrivateKey getPrivateKey(final ProcessContext context, final FlowFile flowFile) {
        final PGPPrivateKeyService privateKeyService = context.getProperty(PRIVATE_KEY_SERVICE).asControllerService(PGPPrivateKeyService.class);
        final long privateKeyId = getPrivateKeyId(context, flowFile);
        final Optional<PGPPrivateKey> optionalPrivateKey = privateKeyService.findPrivateKey(privateKeyId);

        return optionalPrivateKey.orElseThrow(() -> {
            final String message = String.format("Private Key ID [%s] not found", KeyIdentifierConverter.format(privateKeyId));
            return new PGPProcessException(message);
        });
    }

    private long getPrivateKeyId(final ProcessContext context, final FlowFile flowFile) {
        final String privateKeyId = context.getProperty(PRIVATE_KEY_ID).evaluateAttributeExpressions(flowFile).getValue();
        try {
            return KeyIdentifierConverter.parse(privateKeyId);
        } catch (final NumberFormatException e) {
            throw new PGPProcessException(String.format("Private Key ID [%s] Hexadecimal Parsing Failed", privateKeyId), e);
        }
    }

    private CompressionAlgorithm getCompressionAlgorithm(final ProcessContext context) {
        final String algorithm = context.getProperty(COMPRESSION_ALGORITHM).getValue();
        return CompressionAlgorithm.valueOf(algorithm);
    }

    private FileEncoding getFileEncoding(final ProcessContext context) {
        final String encoding = context.getProperty(FILE_ENCODING).getValue();
        return FileEncoding.valueOf(encoding);
    }

    private HashAlgorithm getHashAlgorithm(final ProcessContext context) {
        final String algorithm = context.getProperty(HASH_ALGORITHM).getValue();
        return HashAlgorithm.valueOf(algorithm);
    }

    private SigningStrategy getSigningStrategy(final ProcessContext context) {
        final String strategy = context.getProperty(SIGNING_STRATEGY).getValue();
        return SigningStrategy.valueOf(strategy);
    }

    private class SignStreamCallback extends EncodingStreamCallback {
        private final PGPPrivateKey privateKey;

        private final HashAlgorithm hashAlgorithm;

        private final SigningStrategy signingStrategy;

        private final Map<String, String> attributes = new HashMap<>();

        private SignStreamCallback(final FileEncoding fileEncoding,
                                   final CompressionAlgorithm compressionAlgorithm,
                                   final String filename,
                                   final HashAlgorithm hashAlgorithm,
                                   final SigningStrategy signingStrategy,
                                   final PGPPrivateKey privateKey
        ) {
            super(fileEncoding, compressionAlgorithm, filename);
            this.hashAlgorithm = hashAlgorithm;
            this.signingStrategy = signingStrategy;
            this.privateKey = privateKey;

            attributes.put(PGPAttributeKey.COMPRESS_ALGORITHM, compressionAlgorithm.toString());
            attributes.put(PGPAttributeKey.COMPRESS_ALGORITHM_ID, Integer.toString(compressionAlgorithm.getId()));
            attributes.put(PGPAttributeKey.FILE_ENCODING, fileEncoding.toString());
        }

        /**
         * Process Encoding passing Input Stream through Compression Output Stream
         *
         * @param inputStream          Input Stream
         * @param encodingOutputStream Output Stream configured according to File Encoding
         * @throws IOException  Thrown when unable to read or write streams
         * @throws PGPException Thrown when unable to process compression
         */
        @Override
        protected void processEncoding(final InputStream inputStream, final OutputStream encodingOutputStream) throws IOException, PGPException {
            if (SigningStrategy.DETACHED == signingStrategy) {
                processDetached(inputStream, encodingOutputStream);
            } else {
                super.processEncoding(inputStream, encodingOutputStream);
            }
        }

        /**
         * Process Compression passing Input Stream through Literal Data Output Stream prepended with One-Pass Signature and followed with Signature
         *
         * @param inputStream            Input Stream
         * @param compressedOutputStream Output Stream configured according to Compression Algorithm
         * @throws IOException  Thrown when unable to read or write streams
         * @throws PGPException Thrown when unable to generate signatures
         */
        @Override
        protected void processCompression(final InputStream inputStream, final OutputStream compressedOutputStream) throws IOException, PGPException {
            final PGPSignatureGenerator signatureGenerator = getSignatureGenerator();

            final PGPOnePassSignature onePassSignature = signatureGenerator.generateOnePassVersion(NESTED_SIGNATURE_DISABLED);
            onePassSignature.encode(compressedOutputStream);

            final PGPLiteralDataGenerator literalDataGenerator = new PGPLiteralDataGenerator();
            try (final OutputStream literalOutputStream = openLiteralOutputStream(literalDataGenerator, compressedOutputStream)) {
                processSigned(inputStream, literalOutputStream, signatureGenerator);
            }
            literalDataGenerator.close();

            writeSignature(signatureGenerator, compressedOutputStream);
        }

        private void processDetached(final InputStream inputStream, final OutputStream outputStream) throws IOException, PGPException {
            final PGPSignatureGenerator signatureGenerator = getSignatureGenerator();
            int read;
            while ((read = inputStream.read()) >= 0) {
                signatureGenerator.update((byte) read);
            }
            writeSignature(signatureGenerator, outputStream);
        }

        private void processSigned(final InputStream inputStream, final OutputStream outputStream, final PGPSignatureGenerator signatureGenerator) throws IOException {
            int read;
            while ((read = inputStream.read()) >= 0) {
                outputStream.write(read);
                signatureGenerator.update((byte) read);
            }
        }

        private void writeSignature(final PGPSignatureGenerator signatureGenerator, final OutputStream outputStream) throws PGPException, IOException {
            final PGPSignature signature = signatureGenerator.generate();
            signature.encode(outputStream);
            setSignatureAttributes(signature);
        }

        private PGPSignatureGenerator getSignatureGenerator() throws PGPException {
            final int keyAlgorithm = privateKey.getPublicKeyPacket().getAlgorithm();
            final SecureRandom secureRandom = new SecureRandom();
            final JcaPGPContentSignerBuilder builder = new JcaPGPContentSignerBuilder(keyAlgorithm, hashAlgorithm.getId()).setSecureRandom(secureRandom);
            final PGPSignatureGenerator signatureGenerator = new PGPSignatureGenerator(builder);
            signatureGenerator.init(PGPSignature.BINARY_DOCUMENT, privateKey);
            return signatureGenerator;
        }

        private void setSignatureAttributes(final PGPSignature signature) {
            setSignatureAlgorithm(signature.getKeyAlgorithm(), signature.getHashAlgorithm());
            attributes.put(PGPAttributeKey.SIGNATURE_CREATED, Long.toString(signature.getCreationTime().getTime()));
            attributes.put(PGPAttributeKey.SIGNATURE_KEY_ID, KeyIdentifierConverter.format(signature.getKeyID()));
            attributes.put(PGPAttributeKey.SIGNATURE_TYPE_ID, Integer.toString(signature.getSignatureType()));
            attributes.put(PGPAttributeKey.SIGNATURE_VERSION, Integer.toString(signature.getVersion()));
        }

        private void setSignatureAlgorithm(final int keyAlgorithm, final int hashAlgorithm) {
            attributes.put(PGPAttributeKey.SIGNATURE_HASH_ALGORITHM_ID, Integer.toString(hashAlgorithm));
            attributes.put(PGPAttributeKey.SIGNATURE_KEY_ALGORITHM_ID, Integer.toString(keyAlgorithm));
            try {
                final String algorithm = PGPUtil.getSignatureName(keyAlgorithm, hashAlgorithm);
                attributes.put(PGPAttributeKey.SIGNATURE_ALGORITHM, algorithm);
            } catch (final PGPException e) {
                getLogger().debug("Signature Algorithm Key Identifier [{}] Hash Identifier [{}] not found", keyAlgorithm, hashAlgorithm);
            }
        }
    }
}
