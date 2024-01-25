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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.cipher.algorithm.CipherAlgorithmMode;
import org.apache.nifi.processors.cipher.algorithm.CipherAlgorithmPadding;
import org.apache.nifi.processors.cipher.algorithm.SymmetricCipher;
import org.apache.nifi.processors.cipher.encoded.EncodedDelimiter;
import org.apache.nifi.processors.cipher.encoded.KeySpecificationFormat;
import org.apache.nifi.processors.cipher.io.DecryptStreamCallback;
import org.apache.nifi.security.crypto.key.DerivedKeyParameterSpec;
import org.apache.nifi.security.crypto.key.DerivedKeySpec;
import org.apache.nifi.security.crypto.key.StandardDerivedKeySpec;
import org.apache.nifi.security.crypto.key.detection.DetectedDerivedKeyParameterSpecReader;
import org.apache.nifi.security.crypto.key.detection.DetectedDerivedKeyProvider;
import org.apache.nifi.security.crypto.key.io.ByteBufferSearch;
import org.bouncycastle.util.encoders.Hex;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;
import java.util.List;
import java.util.Set;

@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"cryptography", "decipher", "decrypt", "AES", "Argon2", "bcrypt", "scrypt", "PBKDF2"})
@CapabilityDescription("Decrypt content encrypted with AES and encoded according conventions added in NiFi 0.5.0 for the EncryptContent Processor. " +
        "The Processor reads the first 256 bytes to determine the presence of a cryptographic salt based on finding the 'NiFiSALT' delimiter. " +
        "The salt is not present for content encrypted with a raw hexadecimal key. " +
        "The Processor determines the presence of the initialization vector based on finding the 'NiFiIV' delimiter." +
        "The salt format indicates the Key Derivation Function that the Processor uses to generate a secret key based on a configured password. " +
        "The Processor derives keys with a size of 128 bits according to the conventions implemented in NiFi 0.5.0."
)
public class DecryptContent extends AbstractProcessor {

    static final PropertyDescriptor CIPHER_ALGORITHM_MODE = new PropertyDescriptor.Builder()
            .name("cipher-algorithm-mode")
            .displayName("Cipher Algorithm Mode")
            .description("Block cipher mode of operation for decryption using the Advanced Encryption Standard")
            .required(true)
            .allowableValues(CipherAlgorithmMode.class)
            .defaultValue(CipherAlgorithmMode.GCM)
            .build();

    static final PropertyDescriptor CIPHER_ALGORITHM_PADDING = new PropertyDescriptor.Builder()
            .name("cipher-algorithm-padding")
            .displayName("Cipher Algorithm Padding")
            .description("Padding specification used in cipher operation for decryption using the Advanced Encryption Standard")
            .required(true)
            .allowableValues(CipherAlgorithmPadding.class)
            .defaultValue(CipherAlgorithmPadding.NO_PADDING)
            .build();

    static final PropertyDescriptor KEY_SPECIFICATION_FORMAT = new PropertyDescriptor.Builder()
            .name("key-specification-format")
            .displayName("Key Specification Format")
            .description("Format describing the configured Key Specification")
            .required(true)
            .allowableValues(KeySpecificationFormat.class)
            .defaultValue(KeySpecificationFormat.PASSWORD)
            .build();

    static final PropertyDescriptor KEY_SPECIFICATION = new PropertyDescriptor.Builder()
            .name("key-specification")
            .displayName("Key Specification")
            .description("Specification providing the raw secret key or a password from which to derive a secret key")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Decryption succeeded")
            .build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Decryption failed")
            .build();

    private static final List<PropertyDescriptor> DESCRIPTORS = List.of(
            CIPHER_ALGORITHM_MODE,
            CIPHER_ALGORITHM_PADDING,
            KEY_SPECIFICATION_FORMAT,
            KEY_SPECIFICATION
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(SUCCESS, FAILURE);

    private static final SymmetricCipher SYMMETRIC_CIPHER = SymmetricCipher.AES;

    private static final String TRANSFORMATION_FORMAT = "%s/%s/%s";

    /**
     * Get Supported Property Descriptors
     *
     * @return Processor Property Descriptors
     */
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

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
     * On Trigger decrypts Flow File contents using configured properties
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

        final KeySpecificationFormat keySpecificationFormat = context.getProperty(KEY_SPECIFICATION_FORMAT).asAllowableValue(KeySpecificationFormat.class);

        final String cipherTransformation = getCipherTransformation(context);
        final Cipher cipher = getCipher(cipherTransformation);
        final CipherAlgorithmMode cipherAlgorithmMode = context.getProperty(CIPHER_ALGORITHM_MODE).asAllowableValue(CipherAlgorithmMode.class);

        final KeySpec keySpec = getKeySpec(context, keySpecificationFormat);
        final StreamCallback callback = new DecryptCallback(cipher, cipherAlgorithmMode, keySpec);
        try {
            flowFile = session.write(flowFile, callback);
            getLogger().debug("Decryption completed using [{}] {}", cipherTransformation, flowFile);
            session.transfer(flowFile, SUCCESS);
        } catch (final RuntimeException e) {
            getLogger().error("Decryption failed using [{}] {}", cipherTransformation, flowFile, e);
            session.transfer(flowFile, FAILURE);
        }
    }

    private String getCipherTransformation(final ProcessContext context) {
        final String algorithmMode = context.getProperty(CIPHER_ALGORITHM_MODE).getValue();
        final String algorithmPadding = context.getProperty(CIPHER_ALGORITHM_PADDING).getValue();
        return String.format(TRANSFORMATION_FORMAT, SYMMETRIC_CIPHER.getValue(), algorithmMode, algorithmPadding);
    }

    private Cipher getCipher(final String transformation) {
        try {
            return Cipher.getInstance(transformation);
        } catch (final GeneralSecurityException e) {
            final String message = String.format("Cipher [%s] not found", transformation);
            throw new CipherException(message, e);
        }
    }

    private KeySpec getKeySpec(final ProcessContext context, final KeySpecificationFormat keySpecificationFormat) {
        final KeySpec keySpec;
        final String keySpecification = context.getProperty(KEY_SPECIFICATION).getValue();
        if (KeySpecificationFormat.RAW == keySpecificationFormat) {
            final byte[] decodedKey = Hex.decode(keySpecification);
            keySpec = new SecretKeySpec(decodedKey, SYMMETRIC_CIPHER.getValue());
        } else {
            final char[] password = keySpecification.toCharArray();
            keySpec = new PBEKeySpec(password);
        }
        return keySpec;
    }

    private interface SerializedParameterSpec {

        byte[] getParameters();
    }

    private static class GCMSerializedParameterSpec extends GCMParameterSpec implements SerializedParameterSpec {

        private static final int GCM_TAG_LENGTH_BITS = 128;

        private final byte[] parameters;

        private GCMSerializedParameterSpec(final byte[] iv, final byte[] parameters) {
            super(GCM_TAG_LENGTH_BITS, iv);
            this.parameters = parameters;
        }

        @Override
        public byte[] getParameters() {
            return parameters;
        }
    }

    private static class IvSerializedParameterSpec extends IvParameterSpec implements SerializedParameterSpec {

        private final byte[] parameters;

        private IvSerializedParameterSpec(final byte[] iv, final byte[] parameters) {
            super(iv);
            this.parameters = parameters;
        }

        @Override
        public byte[] getParameters() {
            return parameters;
        }
    }

    private static class DecryptCallback extends DecryptStreamCallback {

        private static final int PARAMETERS_BUFFER_LENGTH = 256;

        private static final int DERIVED_KEY_LENGTH_BYTES = 16;

        private static final int END_OF_FILE = -1;

        private static final byte[] EMPTY_BYTES = {};

        private static final DetectedDerivedKeyParameterSpecReader parameterSpecReader = new DetectedDerivedKeyParameterSpecReader();

        private static final DetectedDerivedKeyProvider derivedKeyProvider = new DetectedDerivedKeyProvider();

        private final CipherAlgorithmMode cipherAlgorithmMode;

        private final KeySpec keySpec;

        private DecryptCallback(
                final Cipher cipher,
                final CipherAlgorithmMode cipherAlgorithmMode,
                final KeySpec keySpec
        ) {
            super(cipher, PARAMETERS_BUFFER_LENGTH);
            this.cipherAlgorithmMode = cipherAlgorithmMode;
            this.keySpec = keySpec;
        }

        /**
         * Read Algorithm Parameters including optional salt bytes for derived keys and required initialization vector bytes
         *
         * @param parameterBuffer Buffer of parameter bytes
         * @return Algorithm Parameters Specification based on Cipher Algorithm Mode and parsed bytes
         */
        @Override
        protected AlgorithmParameterSpec readAlgorithmParameterSpec(final ByteBuffer parameterBuffer) {
            final AlgorithmParameterSpec spec;

            final byte[] salt = readDelimitedBytes(parameterBuffer, EncodedDelimiter.SALT);
            final byte[] iv = readDelimitedBytes(parameterBuffer, EncodedDelimiter.IV);

            if (CipherAlgorithmMode.GCM == cipherAlgorithmMode) {
                spec = new GCMSerializedParameterSpec(iv, salt);
            } else {
                spec = new IvSerializedParameterSpec(iv, salt);
            }

            return spec;
        }

        /**
         * Get Secret Key for cipher operations using either configured hexadecimal key or key derived from password and parameters
         *
         * @param algorithmParameterSpec Algorithm Parameters Specification
         * @return Secret Key for decryption
         */
        @Override
        protected Key getKey(final AlgorithmParameterSpec algorithmParameterSpec) {
            final Key key;

            if (keySpec instanceof SecretKeySpec) {
                key = (SecretKeySpec) keySpec;
            } else if (algorithmParameterSpec instanceof SerializedParameterSpec serializedParameterSpec) {
                final byte[] parameters = serializedParameterSpec.getParameters();
                key = getDerivedKey(parameters);
            } else {
                final String message = String.format("Key Derivation Function Parameters not provided [%s]", algorithmParameterSpec.getClass());
                throw new IllegalArgumentException(message);
            }

            return key;
        }

        private Key getDerivedKey(final byte[] parameters) {
            final DerivedKeyParameterSpec derivedKeyParameterSpec = parameterSpecReader.read(parameters);
            final DerivedKeySpec<DerivedKeyParameterSpec> derivedKeySpec = getDerivedKeySpec(derivedKeyParameterSpec);
            return derivedKeyProvider.getDerivedKey(derivedKeySpec);
        }

        private DerivedKeySpec<DerivedKeyParameterSpec> getDerivedKeySpec(final DerivedKeyParameterSpec parameterSpec) {
            final PBEKeySpec pbeKeySpec = (PBEKeySpec) keySpec;
            final char[] password = pbeKeySpec.getPassword();
            return new StandardDerivedKeySpec<>(
                    password,
                    DERIVED_KEY_LENGTH_BYTES,
                    SYMMETRIC_CIPHER.getValue(),
                    parameterSpec
            );
        }

        private byte[] readDelimitedBytes(final ByteBuffer buffer, final EncodedDelimiter encodedDelimiter) {
            final byte[] delimiter = encodedDelimiter.getDelimiter();
            final int delimiterIndex = ByteBufferSearch.indexOf(buffer, delimiter);

            final byte[] delimitedBytes;
            if (delimiterIndex == END_OF_FILE) {
                delimitedBytes = EMPTY_BYTES;
            } else {
                final int delimitedLength = delimiterIndex - buffer.position();
                delimitedBytes = new byte[delimitedLength];
                buffer.get(delimitedBytes);

                final int newPosition = delimiterIndex + delimiter.length;
                buffer.position(newPosition);
            }
            return delimitedBytes;
        }
    }
}
