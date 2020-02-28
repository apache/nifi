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
package org.apache.nifi.processors.standard.pgp;

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.PGPUtil;
import org.apache.nifi.reporting.InitializationException;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.bcpg.PublicKeyAlgorithmTags;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPPBEEncryptedData;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRing;
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureGenerator;
import org.bouncycastle.openpgp.PGPSignatureList;
import org.bouncycastle.openpgp.jcajce.JcaPGPObjectFactory;
import org.bouncycastle.openpgp.jcajce.JcaPGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.operator.KeyFingerPrintCalculator;
import org.bouncycastle.openpgp.operator.PBEDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.PGPDigestCalculatorProvider;
import org.bouncycastle.openpgp.operator.PublicKeyDataDecryptorFactory;
import org.bouncycastle.openpgp.operator.bc.BcKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.bc.BcPGPDigestCalculatorProvider;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentSignerBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPContentVerifierBuilderProvider;
import org.bouncycastle.openpgp.operator.jcajce.JcaPGPDigestCalculatorProviderBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePBEDataDecryptorFactoryBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePBEKeyEncryptionMethodGenerator;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePGPDataEncryptorBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyDataDecryptorFactoryBuilder;
import org.bouncycastle.openpgp.operator.jcajce.JcePublicKeyKeyEncryptionMethodGenerator;
import org.bouncycastle.util.io.Streams;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.Deflater;


/**
 * This class defines an implementation of {@link PGPService} suitable for use with the PGP processors in this package.
 *
 * In addition to basic Controller Service functionality this class exposes high-level, stream-based cryptographic operations
 * to clients.  These operations are encrypt, decrypt, sign, and verify.
 *
 * This implementation also provides the options builder methods.  These methods build the various options instances that
 * correspond to the cryptographic operations.  These option instances allow the controller service to combine the properties
 * of the processors with the properties of the controller, and then use that combination to parameterize the given operation.
 * Note that this also keeps the keys and passphrases out of processor code completely.
 *
 * This class also has a set of private methods for reading key material from disk or input streams.  These methods are
 * not exposed to the processor clients (but are only package-private at the moment).  Note that the key handling methods
 * support caching.
 *
 * The net effect of combining these behaviors is to reduce the significant coupling to the BC/PGP libraries to just
 * this class.  This was done intentionally to make maintenance and auditing easier.
 *
 */
@CapabilityDescription("Defines cryptographic key material and cryptographic operations for PGP processors.")
@Tags({"pgp", "gpg", "encryption", "credentials", "provider"})
@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.ACCESS_ENCRYPTION_KEY,
                        explanation = "Provides operator the ability to access and use encryption keys assuming all permissions that NiFi has.")
        }
)
public class PGPControllerService extends AbstractControllerService implements PGPService {
    final static String CONTROLLER_NAME = "PGP Controller Service";

    // Uncertain if these could ever collide, so err on the side of caution can keep them distinct:
    private final static Map<Integer, PGPPublicKeys> publicKeyCache = new HashMap<>();
    private final static Map<Integer, PGPSecretKeys> secretKeyCache = new HashMap<>();

    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    public static final PropertyDescriptor PUBLIC_KEYRING_FILE = new PropertyDescriptor.Builder()
            .name("public-keyring-file")
            .displayName("Public Key or Keyring File")
            .description("PGP public key or keyring file.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor PUBLIC_KEYRING_TEXT = new PropertyDescriptor.Builder()
            .name("public-keyring-text")
            .displayName("Public Key or Keyring Text")
            .description("PGP public key or keyring as text (also called Armored text or ASCII text).")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PUBLIC_KEY_USER_ID = new PropertyDescriptor.Builder()
            .name("public-key-user-id")
            .displayName("Public Key User ID")
            .description("Public Key user ID (also called ID or Name) for the key within the public key keyring.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SECRET_KEYRING_FILE = new PropertyDescriptor.Builder()
            .name("secret-keyring-file")
            .displayName("Secret Key or Keyring File")
            .description("PGP secret key or keyring file.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor SECRET_KEYRING_TEXT = new PropertyDescriptor.Builder()
            .name("secret-keyring-text")
            .displayName("Secret Key or Keyring Text")
            .description("PGP secret key or keyring as text (also called Armored text or ASCII text).")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor SECRET_KEY_USER_ID = new PropertyDescriptor.Builder()
            .name("secret-key-user-id")
            .displayName("Secret Key User ID")
            .description("Secret Key user ID (also called ID or Name) for the key within the secret keyring.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PRIVATE_KEY_PASSPHRASE = new PropertyDescriptor.Builder()
            .name("private-key-passphrase")
            .displayName("Private Key Passphrase")
            .description("This is the passphrase for the specified secret key.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor PBE_PASSPHRASE = new PropertyDescriptor.Builder()
            .name("pbe-passphrase")
            .displayName("Encryption Passphrase")
            .description("This is the passphrase for password-based encryption and decryption.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();


    @Override
    protected void init(ControllerServiceInitializationContext config) throws InitializationException {
        publicKeyCache.clear();
        secretKeyCache.clear();
        super.init(config);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PUBLIC_KEYRING_FILE);
        properties.add(PUBLIC_KEYRING_TEXT);
        properties.add(PUBLIC_KEY_USER_ID);
        properties.add(SECRET_KEYRING_FILE);
        properties.add(SECRET_KEYRING_TEXT);
        properties.add(SECRET_KEY_USER_ID);
        properties.add(PRIVATE_KEY_PASSPHRASE);
        properties.add(PBE_PASSPHRASE);
        return properties;
    }


    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        if (validateForEncrypt(validationContext).isEmpty()) return Collections.emptySet();

        if (validateForDecrypt(validationContext).isEmpty()) return Collections.emptySet();

        if (validateForSign(validationContext).isEmpty()) return Collections.emptySet();

        if (validateForVerify(validationContext).isEmpty()) return Collections.emptySet();

        final List<ValidationResult> problems = new ArrayList<>();
        problems.add(new ValidationResult.Builder()
                .subject(CONTROLLER_NAME)
                .valid(false)
                .explanation("Controller not configured for any operation.")
                .build());
        return problems;
    }


    /**
     * Read from an {@link InputStream} and write an encrypted representation to an {@link OutputStream}.
     *
     * @param input plain data
     * @param output to receive encrypted data
     * @param options used to configure encryption operation
     */
    public void encrypt(InputStream input, OutputStream output, EncryptOptions options) throws IOException, PGPException {
        final int bufferSize = PGPUtil.BUFFER_SIZE;
        PGPEncryptedDataGenerator generator = options.getDataGenerator();
        boolean armor = options.getArmor();
        OutputStream out = armor ? new ArmoredOutputStream(output) : output;

        try (OutputStream encryptedOutput = generator.open(out, new byte[bufferSize])) {
            try (OutputStream compressedOutput = new PGPCompressedDataGenerator(PGPCompressedData.ZIP, Deflater.BEST_SPEED)
                    .open(encryptedOutput, new byte[bufferSize])) {
                try (OutputStream literalOutput = new PGPLiteralDataGenerator()
                        .open(compressedOutput, PGPLiteralData.BINARY, "", new Date(), new byte[bufferSize])) {
                    Streams.pipeAll(input, literalOutput);
                }
            }
        } finally {
            if (armor)
                out.close();
        }
    }


    /**
     * Read from an encrypted {@link InputStream} and write a decrypted representation to an {@link OutputStream}.
     *
     * @param input encrypted data
     * @param output to receive decrypted data
     * @param options used to configure decryption operation
     */
    public void decrypt(InputStream input, OutputStream output, DecryptOptions options) throws IOException {
        try (InputStream decodedInput = org.bouncycastle.openpgp.PGPUtil.getDecoderStream(input)) {
            PGPObjectFactory inputFactory = new PGPObjectFactory(decodedInput, new BcKeyFingerprintCalculator());
            Object head = inputFactory.nextObject();

            if (!(head instanceof PGPEncryptedDataList)) {
                head = inputFactory.nextObject();
                if (!(head instanceof PGPEncryptedDataList)) {
                    throw new IOException("Input stream does not contain PGP encrypted data.");
                }
            }

            try {
                final Iterator objects = ((PGPEncryptedDataList) head).getEncryptedDataObjects();
                PGPEncryptedData data;

                while (objects.hasNext()) {
                    Object obj = objects.next();
                    InputStream clearInput;

                    if (obj instanceof PGPPublicKeyEncryptedData) {
                        data = (PGPEncryptedData) obj;
                    } else if (obj instanceof PGPPBEEncryptedData) {
                        data = (PGPEncryptedData) obj;
                    } else {
                        throw new IOException("Expected encrypted data");
                    }
                    clearInput = options.getInputStream(data);

                    JcaPGPObjectFactory plainFactory = new JcaPGPObjectFactory(clearInput);
                    Object plain = plainFactory.nextObject();

                    if (plain instanceof PGPCompressedData) {
                        PGPCompressedData compressed = (PGPCompressedData) plain;
                        plain = (new JcaPGPObjectFactory(compressed.getDataStream())).nextObject();
                    }

                    if (plain instanceof PGPLiteralData) {
                        PGPLiteralData literalObject = (PGPLiteralData) plain;
                        try (InputStream literalInput = literalObject.getInputStream()) {
                            Streams.pipeAll(literalInput, output);
                        }
                    } else {
                        throw new PGPException("message is not a simple encrypted file - type unknown.");
                    }

                    if (data.isIntegrityProtected()) {
                        if (!data.verify()) {
                            throw new PGPException("Failed message integrity check");
                        }
                    } else {
                        throw new PGPException("Encrypted data packet does not have integrity check.");
                    }
                }
            } catch (final Exception e) {
                throw new IOException(e.getMessage());
            }
        }
    }


    /**
     * Read from an {@link InputStream} to generate a signature written to an {@link OutputStream}.
     *
     * @param input clear or cipher data
     * @param signature to receive signature
     * @param options used to configure sign operation
     */
    public void sign(InputStream input, OutputStream signature, SignOptions options) throws IOException, PGPException {
        if (options == null || options.getPrivateKey() == null) {
            throw new IOException("Sign operation invalid without Private Key");
        }
        JcaPGPContentSignerBuilder builder = new JcaPGPContentSignerBuilder(PublicKeyAlgorithmTags.RSA_GENERAL, options.getHashAlgorithm()).setProvider("BC");
        PGPSignatureGenerator generator = new PGPSignatureGenerator(builder);
        generator.init(PGPSignature.BINARY_DOCUMENT, options.getPrivateKey());
        readAndUpdate(input, generator);
        generator.generate().encode(signature);
    }


    /**
     * Read from an {@link InputStream} to verify its signature.
     *
     * @param input clear or cipher data
     * @param signature signature data
     * @param options used to configure verify operation
     * @return true if the signature matches
     */
    public boolean verify(InputStream input, InputStream signature, VerifyOptions options) throws IOException, PGPException {
        JcaPGPObjectFactory signatureFactory = new JcaPGPObjectFactory(org.bouncycastle.openpgp.PGPUtil.getDecoderStream(signature));
        PGPSignatureList signatures;

        Object head = signatureFactory.nextObject();
        if (head instanceof PGPCompressedData) {
            PGPCompressedData compressed = (PGPCompressedData) head;
            signatureFactory = new JcaPGPObjectFactory(compressed.getDataStream());
            signatures = (PGPSignatureList) signatureFactory.nextObject();
        } else if (head instanceof  PGPSignatureList) {
            signatures = (PGPSignatureList) head;
        } else {
            throw new PGPException("Not a signature stream.");
        }

        PGPSignature innerSignature = signatures.get(0);
        innerSignature.init(new JcaPGPContentVerifierBuilderProvider().setProvider("BC"), options.getPublicKey());
        readAndUpdate(input, innerSignature);
        return innerSignature.verify();
    }


    /**
     * Generate an {@link EncryptOptions} instance for an encrypt operation.
     *
     * @param algorithm encryption algorithm identifier, see {@link org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags}
     * @param armor if true, encrypted data will be PGP-encoded ascii; if false, encrypted data will not be encoded ("raw")
     * @return encrypt options
     */
    @Override
    public EncryptOptions optionsForEncrypt(int algorithm, boolean armor) {
        PGPEncryptedDataGenerator generator = new PGPEncryptedDataGenerator(new JcePGPDataEncryptorBuilder(algorithm)
                .setWithIntegrityPacket(true)
                .setProvider("BC"));
        char[] passphrase = getPBEPassPhrase(getConfigurationContext());
        if (passphrase != null && passphrase.length != 0) {
            generator.addMethod(new JcePBEKeyEncryptionMethodGenerator(passphrase).setProvider("BC"));
        } else {
            generator.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(getPublicKey(getConfigurationContext())).setProvider("BC"));
        }

        return new EncryptOptions() {
            @Override
            public PGPEncryptedDataGenerator getDataGenerator() {
                return generator;
            }

            @Override
            public boolean getArmor() {
                return armor;
            }
        };
    }


    /**
     * Generate a {@link DecryptOptions} instance for a decrypt operation.
     *
     * @return decrypt options
     */
    @Override
    public DecryptOptions optionsForDecrypt() {
        PGPDigestCalculatorProvider calc = null;
        try {
            calc = new JcaPGPDigestCalculatorProviderBuilder().setProvider("BC").build();
        } catch (PGPException e) {
            calc = new BcPGPDigestCalculatorProvider();
        }

        char[] passphrase = getPBEPassPhrase(getConfigurationContext());
        if (passphrase != null && passphrase.length != 0) {
            PBEDataDecryptorFactory factory = new JcePBEDataDecryptorFactoryBuilder(calc).setProvider("BC").build(passphrase);
            return packet -> ((PGPPBEEncryptedData) packet).getDataStream(factory);
        } else {
            PublicKeyDataDecryptorFactory factory = new JcePublicKeyDataDecryptorFactoryBuilder().setProvider("BC").build(getPrivateKey(getConfigurationContext()));
            return packet -> ((PGPPublicKeyEncryptedData) packet).getDataStream(factory);
        }
    }


    /**
     * Generate a {@link SignOptions} instance for a sign operation.
     *
     * @param algorithm signature algorithm identifier, see {@link org.bouncycastle.bcpg.HashAlgorithmTags}
     * @return sign options
     */
    @Override
    public SignOptions optionsForSign(int algorithm) {
        PGPPrivateKey privateKey = getPrivateKey(getConfigurationContext());

        return new SignOptions() {
            @Override
            public PGPPrivateKey getPrivateKey() {
                return privateKey;
            }

            @Override
            public int getHashAlgorithm() {
                return algorithm;
            }
        };
    }


    /**
     * Generate a {@link VerifyOptions} instance for a verify operation.
     *
     * @return verify options
     */
    @Override
    public VerifyOptions optionsForVerify() {
        PGPPublicKey publicKey = getPublicKey(getConfigurationContext());
        return new VerifyOptions() {
            @Override
            public PGPPublicKey getPublicKey() {
                return publicKey;
            }
        };
    }


    /**
     * This method validates a {@link PropertyContext} for encryption.
     *
     * @param context {@link PropertyContext} describing encryption keys
     * @return validation results, which may be empty if given context is valid
     */
    private Collection<ValidationResult> validateForEncrypt(PropertyContext context) {
        PGPPublicKey key = getPublicKey(context);
        if (key != null && key.isEncryptionKey()) {
            return Collections.emptySet();
        }

        char[] passphrase = getPBEPassPhrase(context);
        if (passphrase != null) {
            return Collections.emptySet();
        }

        final List<ValidationResult> problems = new ArrayList<>();
        problems.add(new ValidationResult.Builder()
                .subject(CONTROLLER_NAME)
                .valid(false)
                .explanation("Controller requires a public key or PBE passphrase for Encrypt operations.")
                .build());
        return problems;
    }


    /**
     * This method validates a {@link PropertyContext} for decryption.
     *
     * @param context {@link PropertyContext} describing decryption keys
     * @return validation results, which may be empty if given context is valid
     */
    private Collection<ValidationResult> validateForDecrypt(PropertyContext context) {
        PGPPrivateKey key = getPrivateKey(context);
        if (key != null) {
            return Collections.emptySet();
        }

        char[] passphrase = getPBEPassPhrase(context);
        if (passphrase != null) {
            return Collections.emptySet();
        }

        final List<ValidationResult> problems = new ArrayList<>();
        problems.add(new ValidationResult.Builder()
                .subject(CONTROLLER_NAME)
                .valid(false)
                .explanation("Controller requires a secret key or PBE passphrase for Decrypt operations.")
                .build());
        return problems;
    }


    /**
     * This method validates a {@link PropertyContext} for signing.
     *
     * @param context describing signing keys
     * @return validation results, which may be empty if given context is valid
     */
    private Collection<ValidationResult> validateForSign(PropertyContext context) {
        PGPPrivateKey key = getPrivateKey(context);
        if (key != null) {
            return Collections.emptySet();
        }

        final List<ValidationResult> problems = new ArrayList<>();
        problems.add(new ValidationResult.Builder()
                .subject(CONTROLLER_NAME)
                .valid(false)
                .explanation("Controller requires a secret key for Sign operations.")
                .build());
        return problems;
    }


    /**
     * This method validates a {@link PropertyContext} for verifying.
     *
     * @param context describing verify keys
     * @return validation results, which may be empty if given context is valid
     */
    private Collection<ValidationResult> validateForVerify(PropertyContext context) {
        PGPPublicKey key = getPublicKey(context);
        if (key != null && key.isEncryptionKey()) {
            return Collections.emptySet();
        }

        final List<ValidationResult> problems = new ArrayList<>();
        problems.add(new ValidationResult.Builder()
                .subject(CONTROLLER_NAME)
                .valid(false)
                .explanation("Controller requires a public key for Verify operations.")
                .build());
        return problems;
    }


    private PGPPublicKey getPublicKey(PropertyContext context) {
        PGPPublicKeys keys = null;
        final ComponentLog logger = this.getLogger();

        if (context.getProperty(PUBLIC_KEYRING_TEXT).isSet()) {
            final String keyText = context.getProperty(PUBLIC_KEYRING_TEXT).getValue();
            keys = readPublicKeys(keyText);

        } else if (context.getProperty(PUBLIC_KEYRING_FILE).isSet()) {
            final String keyFile = context.getProperty(PUBLIC_KEYRING_FILE).getValue();
            try {
                keys = readPublicKeys(new File(keyFile));
            } catch (final FileNotFoundException e) {
                logger.debug("Public key ring file not found: " + e);
                return null;
            }
        }

        if (keys == null || keys.size() == 0) {
            logger.debug("Public key ring empty.");
            return null;
        }

        if (context.getProperty(PUBLIC_KEY_USER_ID).isSet()) {
            return keys.getPublicKey(context.getProperty(PUBLIC_KEY_USER_ID).getValue());
        }

        for (PGPPublicKey key : keys.values()) {
            if (key.isEncryptionKey())
                return key;
        }

        logger.debug("Unable to find public key from context.");
        return null;
    }


    private PGPPrivateKey getPrivateKey(PropertyContext context) {
        String secretKeyUserId = null;
        char[] privateKeyPassPhrase = null;
        PGPSecretKeys keys = null;
        PBESecretKeyDecryptor decryptor = null;
        final ComponentLog logger = this.getLogger();

        if (context.getProperty(SECRET_KEY_USER_ID).isSet()) {
            secretKeyUserId = context.getProperty(SECRET_KEY_USER_ID).getValue();
        }

        if (context.getProperty(PRIVATE_KEY_PASSPHRASE).isSet()) {
            privateKeyPassPhrase = context.getProperty(PRIVATE_KEY_PASSPHRASE).getValue().toCharArray();

            try {
                decryptor = new JcePBESecretKeyDecryptorBuilder().setProvider("BC").build(privateKeyPassPhrase);
            } catch (final PGPException e) {
                logger.debug("Unable to build PBE decryptor: " + e);
                return null;
            }
        }

        if (context.getProperty(SECRET_KEYRING_TEXT).isSet()) {
            final String secretKeyText = context.getProperty(SECRET_KEYRING_TEXT).getValue();
            keys = readSecretKeys(secretKeyText);

        } else  if (context.getProperty(SECRET_KEYRING_FILE).isSet()) {
            final String keyFile = context.getProperty(SECRET_KEYRING_FILE).getValue();
            try {
                keys = readSecretKeys(new File(keyFile));
            } catch (final FileNotFoundException e) {
                logger.debug("Secret key ring file not found: " + e);
                return null;
            }
        }

        if (keys == null || keys.size() == 0) {
            logger.debug("Secret key ring empty.");
            return null;
        }

        if (secretKeyUserId != null) {
            try {
                return keys.getSecretKey(secretKeyUserId).extractPrivateKey(decryptor);
            } catch (final PGPException e) {
                logger.debug("Unable to extract private key from secret key: " + e);
                return null;
            }
        }

        for (PGPSecretKey key : keys.values()) {
            if (!key.isPrivateKeyEmpty()) {
                try {
                    return key.extractPrivateKey(decryptor);
                } catch (final Exception e) {
                    logger.debug("Unable to extract private key: " + e);
                }
            }
        }

        logger.debug("Unable to find private key from context.");
        return null;
    }


    private char[] getPBEPassPhrase(PropertyContext context) {
        if (context.getProperty(PBE_PASSPHRASE).isSet()) {
            return context.getProperty(PBE_PASSPHRASE).getValue().toCharArray();
        }
        return null;
    }


    static class PGPPublicKeys extends HashMap<String, PGPPublicKey> {
        /**
         * Returns public key matching the given key id or null.
         *
         * @param keyID public key id to match
         * @return public key matching given key ID or null
         */
        public PGPPublicKey getPublicKey(long keyID) {
            for (PGPPublicKey key : this.values()) {
                if (key.getKeyID() == keyID) {
                    return key;
                }
            }
            return null;
        }

        /**
         * Returns public key matching the given user id or null.
         *
         * @param userID public key user id to match
         * @return public key matching given user ID or null
         */
        PGPPublicKey getPublicKey(String userID) {
            return this.get(userID);
        }
    }

    static class PGPSecretKeys extends HashMap<String, PGPSecretKey> {
        /**
         * Returns secret key matching the given key id or null.
         *
         * @param keyID secret key id to match
         * @return secret key matching given key ID or null
         */
        PGPSecretKey getSecretKey(long keyID) {
            for (PGPSecretKey key : this.values()) {
                if (key.getKeyID() == keyID) {
                    return key;
                }
            }
            return null;
        }

        /**
         * Returns secret key matching the given user id or null.
         *
         * @param userID secret key user id to match
         * @return secret key matching given user ID or null
         */
        PGPSecretKey getSecretKey(String userID) {
            return this.get(userID);
        }
    }


    /**
     * Creates a list of public keys from the input stream.
     *
     * @param input public key or key ring stream
     * @param hash value to use as cache key
     * @return public keys
     */
    static PGPPublicKeys readPublicKeys(InputStream input, Integer hash) {
        if (hash != null && publicKeyCache.containsKey(hash)) {
            return publicKeyCache.get(hash);
        }

        PGPPublicKeys keys = new PGPPublicKeys();
        JcaPGPPublicKeyRingCollection rings;

        try {
            rings = new JcaPGPPublicKeyRingCollection(org.bouncycastle.openpgp.PGPUtil.getDecoderStream(input));
        } catch (final IOException | PGPException ignored) {
            return null;
        }

        for (PGPPublicKeyRing ring : rings) {
            for (PGPPublicKey key : ring) {
                for (Iterator<String> it = key.getUserIDs(); it.hasNext(); ) {
                    final String userID = it.next();
                    keys.put(userID, key);
                }
            }
        }

        if (hash != null && keys.size() > 0) publicKeyCache.put(hash, keys);
        return keys;
    }

    /**
     * Creates a list of public keys from the input stream.  When called with a lone {@link InputStream}, this method
     * will not cache the resulting keys.
     *
     * @param input public key or key ring stream
     * @return public keys
     */
    static PGPPublicKeys readPublicKeys(InputStream input) {
        return readPublicKeys(input, null);
    }

    /**
     * Creates a list of public keys from the given String.
     *
     * @param source public key text
     * @return pubic keys
     */
    static PGPPublicKeys readPublicKeys(String source) {
        return readPublicKeys(new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8)), source.hashCode());
    }

    /**
     * Creates a list of public keys from the given {@link File}.
     *
     * @param source public key File
     * @return public keys
     */
    static PGPPublicKeys readPublicKeys(File source) throws FileNotFoundException {
        return readPublicKeys(new FileInputStream(source), source.getAbsoluteFile().hashCode());
    }


    /**
     * Creates a list of secret keys from the input stream.
     *
     * @param input secret key or key ring stream
     * @return secret keys
     */
    static PGPSecretKeys readSecretKeys(InputStream input, Integer hash) {
        if (hash != null && secretKeyCache.containsKey(hash)) {
            return secretKeyCache.get(hash);
        }

        PGPSecretKeys keys = new PGPSecretKeys();
        KeyFingerPrintCalculator calc = new BcKeyFingerprintCalculator();
        PGPSecretKeyRingCollection rings;

        try {
            rings = new PGPSecretKeyRingCollection(org.bouncycastle.openpgp.PGPUtil.getDecoderStream(input), calc);
        } catch (final IOException | PGPException ignored) {
            return null;
        }

        for (PGPSecretKeyRing ring : rings) {
            for (PGPSecretKey secretKey : ring) {
                for (Iterator<String> it = secretKey.getUserIDs(); it.hasNext(); ) {
                    final String userID = it.next();
                    keys.put(userID, secretKey);
                }
            }
        }
        if (hash != null && keys.size() > 0) secretKeyCache.put(hash, keys);
        return keys;
    }

    /**
     * Creates a list of secret keys from the input stream.  When called with a lone {@link InputStream}, this method
     * will not cache the resulting keys.
     *
     * @param input secret key or key ring stream
     * @return secret keys
     */
    static PGPSecretKeys readSecretKeys(InputStream input) {
        return readSecretKeys(input, null);
    }


    /**
     * Creates a list of secret keys from the input stream.
     *
     * @param source secret key text
     * @return secret keys
     */

    static PGPSecretKeys readSecretKeys(String source) {
        return readSecretKeys(new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8)), source.hashCode());
    }


    /**
     * Creates a list of secret keys from the input stream.
     *
     * @param source secret key File
     * @return secret keys
     */
    static PGPSecretKeys readSecretKeys(File source) throws FileNotFoundException {
        return readSecretKeys(new FileInputStream(source), source.getAbsolutePath().hashCode());
    }


    /**
     * Read an {@link InputStream} whilst updating a {@link PGPSignature}.
     *
     * @param input stream to read and sign
     * @param signature resulting signature
     */
    private static void readAndUpdate(InputStream input, PGPSignature signature) throws IOException {
        int i;
        while ((i = input.read()) >= 0) {
            signature.update((byte) i);
        }
        input.close();
    }


    /**
     * Read an {@link InputStream} whilst updating a {@link PGPSignatureGenerator}.
     *
     * @param input stream to read and sign
     * @param signature resulting signature
     */
    private static void readAndUpdate(InputStream input, PGPSignatureGenerator signature) throws IOException {
        int i;
        while ((i = input.read()) >= 0) {
            signature.update((byte) i);
        }
        input.close();
    }
}
