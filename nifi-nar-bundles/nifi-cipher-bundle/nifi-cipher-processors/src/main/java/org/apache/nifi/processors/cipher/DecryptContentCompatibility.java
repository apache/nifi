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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
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
import org.apache.nifi.processors.cipher.compatibility.CompatibilityModeEncryptionScheme;
import org.apache.nifi.processors.cipher.compatibility.CompatibilityModeKeyDerivationStrategy;
import org.apache.nifi.processors.cipher.io.DecryptStreamCallback;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.InvalidKeySpecException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SideEffectFree
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"cryptography", "decipher", "decrypt", "Jasypt", "OpenSSL", "PKCS5", "PBES1"})
@CapabilityDescription("Decrypt content using password-based encryption schemes with legacy algorithms supporting historical compatibility modes.")
@WritesAttributes({
        @WritesAttribute(attribute = CipherAttributeKey.PBE_SCHEME, description = "Password-Based Encryption Scheme"),
        @WritesAttribute(attribute = CipherAttributeKey.PBE_SYMMETRIC_CIPHER, description = "Password-Based Encryption Block Cipher"),
        @WritesAttribute(attribute = CipherAttributeKey.PBE_DIGEST_ALGORITHM, description = "Password-Based Encryption Digest Algorithm"),
})
public class DecryptContentCompatibility extends AbstractProcessor {

    static final PropertyDescriptor ENCRYPTION_SCHEME = new PropertyDescriptor.Builder()
            .name("encryption-scheme")
            .displayName("Encryption Scheme")
            .description("Password-Based Encryption Scheme including PBES1 described in RFC 8018, and others defined according to PKCS12 and Bouncy Castle implementations")
            .required(true)
            .allowableValues(CompatibilityModeEncryptionScheme.class)
            .build();

    static final PropertyDescriptor KEY_DERIVATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("key-derivation-strategy")
            .displayName("Key Derivation Strategy")
            .description("Strategy for reading salt from encoded contents and deriving the decryption key according to the number of function iterations")
            .required(true)
            .allowableValues(CompatibilityModeKeyDerivationStrategy.class)
            .build();

    static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("password")
            .displayName("Password")
            .description("Password required for Password-Based Encryption Schemes")
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
            ENCRYPTION_SCHEME,
            KEY_DERIVATION_STRATEGY,
            PASSWORD
    );

    private static final Set<Relationship> RELATIONSHIPS = Set.of(SUCCESS, FAILURE);

    private static final BouncyCastleProvider BOUNCY_CASTLE_PROVIDER = new BouncyCastleProvider();

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

        final CompatibilityModeEncryptionScheme encryptionScheme =
                context.getProperty(ENCRYPTION_SCHEME).asAllowableValue(CompatibilityModeEncryptionScheme.class);
        final String scheme = encryptionScheme.getValue();
        final Cipher cipher = getCipher(scheme);

        final char[] password = context.getProperty(PASSWORD).getValue().toCharArray();
        final PBEKeySpec keySpec = new PBEKeySpec(password);

        final CompatibilityModeKeyDerivationStrategy keyDerivationStrategy =
                context.getProperty(KEY_DERIVATION_STRATEGY).asAllowableValue(CompatibilityModeKeyDerivationStrategy.class);
        final StreamCallback callback = new DecryptCallback(cipher, keySpec, keyDerivationStrategy);

        final Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put(CipherAttributeKey.PBE_SCHEME, encryptionScheme.getValue());
        attributes.put(CipherAttributeKey.PBE_SYMMETRIC_CIPHER, encryptionScheme.getSymmetricCipher().getValue());
        attributes.put(CipherAttributeKey.PBE_DIGEST_ALGORITHM, encryptionScheme.getDigestAlgorithm().getValue());

        try {
            flowFile = session.write(flowFile, callback);
            flowFile = session.putAllAttributes(flowFile, attributes);
            getLogger().debug("Decryption completed using [{}] {}", scheme, flowFile);
            session.transfer(flowFile, SUCCESS);
        } catch (final RuntimeException e) {
            getLogger().error("Decryption failed using [{}] {}", scheme, flowFile, e);
            session.transfer(flowFile, FAILURE);
        }
    }

    private Cipher getCipher(final String transformation) {
        try {
            return Cipher.getInstance(transformation, BOUNCY_CASTLE_PROVIDER);
        } catch (final GeneralSecurityException e) {
            final String message = String.format("Cipher [%s] not found", transformation);
            throw new CipherException(message, e);
        }
    }

    private static class DecryptCallback extends DecryptStreamCallback {
        private static final byte[] EMPTY_SALT = {};

        private static final int BLOCK_SIZE_UNDEFINED = 0;

        private final String cipherAlgorithm;

        private final int cipherBlockSize;

        private final PBEKeySpec keySpec;

        private final CompatibilityModeKeyDerivationStrategy keyDerivationStrategy;

        private DecryptCallback(
                final Cipher cipher,
                final PBEKeySpec keySpec,
                final CompatibilityModeKeyDerivationStrategy keyDerivationStrategy
        ) {
            super(cipher, keyDerivationStrategy.getSaltBufferLength());
            this.cipherAlgorithm = cipher.getAlgorithm();
            this.cipherBlockSize = cipher.getBlockSize();
            this.keySpec = keySpec;
            this.keyDerivationStrategy = keyDerivationStrategy;
        }

        /**
         * Get Secret Key generated based on configured key specification
         *
         * @param algorithmParameterSpec Algorithm Parameters Specification not used
         * @return Secret Key
         */
        @Override
        protected Key getKey(final AlgorithmParameterSpec algorithmParameterSpec) {
            final SecretKeyFactory secretKeyFactory = getSecretKeyFactory();
            try {
                return secretKeyFactory.generateSecret(keySpec);
            } catch (final InvalidKeySpecException e) {
                final String message = String.format("Generate Secret Key Algorithm [%s] invalid specification", cipherAlgorithm);
                throw new CipherException(message, e);
            }
        }

        /**
         * Read salt bytes based on configured key derivation strategy
         *
         * @param parameterBuffer Buffer of parameter bytes
         * @return Password-Based Encryption specification with salt and configured iterations
         */
        @Override
        protected AlgorithmParameterSpec readAlgorithmParameterSpec(final ByteBuffer parameterBuffer) {
            final byte[] salt = readSalt(parameterBuffer);
            return new PBEParameterSpec(salt, keyDerivationStrategy.getIterations());
        }

        private byte[] readSalt(final ByteBuffer byteBuffer) {
            final byte[] salt;

            if (CompatibilityModeKeyDerivationStrategy.OPENSSL_EVP_BYTES_TO_KEY == keyDerivationStrategy) {
                salt = readSaltOpenSsl(byteBuffer);
            } else {
                salt = readSaltStandard(byteBuffer);
            }

            return salt;
        }

        private byte[] readSaltOpenSsl(final ByteBuffer byteBuffer) {
            final byte[] salt;

            final int saltHeaderLength = keyDerivationStrategy.getSaltHeader().length;
            final byte[] saltHeader = new byte[saltHeaderLength];
            byteBuffer.get(saltHeader);

            if (MessageDigest.isEqual(keyDerivationStrategy.getSaltHeader(), saltHeader)) {
                salt = new byte[keyDerivationStrategy.getSaltStandardLength()];
                byteBuffer.get(salt);
            } else {
                salt = EMPTY_SALT;
                byteBuffer.rewind();
            }

            return salt;
        }

        private byte[] readSaltStandard(final ByteBuffer byteBuffer) {
            final int saltLength = cipherBlockSize == BLOCK_SIZE_UNDEFINED ? keyDerivationStrategy.getSaltStandardLength() : cipherBlockSize;
            final byte[] salt = new byte[saltLength];
            byteBuffer.get(salt);
            return salt;
        }

        private SecretKeyFactory getSecretKeyFactory() {
            try {
                return SecretKeyFactory.getInstance(cipherAlgorithm, BOUNCY_CASTLE_PROVIDER);
            } catch (final NoSuchAlgorithmException e) {
                final String message = String.format("Secret Key Factory Algorithm [%s] not found", cipherAlgorithm);
                throw new CipherException(message, e);
            }
        }
    }
}
