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
package org.apache.nifi.security.pgp;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.pgp.controllerservices.PGPKeyMaterialService;
import org.apache.nifi.processor.util.StandardValidators;
import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.bcpg.HashAlgorithmTags;
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
import org.bouncycastle.openpgp.PGPUtil;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.Deflater;


public class StandardPGPOperator implements PGPOperator {
    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    private static final Logger logger = LoggerFactory.getLogger(StandardPGPOperator.class);
    public static final String DEFAULT_SIGNATURE_ATTRIBUTE = "content-signature";

    public static final PropertyDescriptor PGP_KEY_SERVICE = new PropertyDescriptor.Builder()
            .name("pgp-key-service")
            .displayName("PGP Key Material Service")
            .description("PGP key material service for using cryptographic keys and passphrases.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .identifiesControllerService(PGPKeyMaterialService.class)
            .build();

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

    public static final PropertyDescriptor ENCRYPT_ALGORITHM = new PropertyDescriptor.Builder()
            .name("encrypt-algorithm")
            .displayName("Encryption Cipher Algorithm")
            .description("The cipher algorithm used when encrypting data.")
            .allowableValues(getCipherAllowableValues())
            .defaultValue(getCipherDefaultValue())
            .build();

    public static final PropertyDescriptor ENCRYPT_ENCODING = new PropertyDescriptor.Builder()
            .name("encrypt-encoding")
            .displayName("Encryption Data Encoding")
            .description("The data encoding method used when writing encrypting data.")
            .allowableValues(
                    new AllowableValue("0", "Raw (bytes with no encoding)"),
                    new AllowableValue("1", "PGP Armor (encoded text)"))
            .defaultValue("0")
            .build();

    public static final PropertyDescriptor SIGNATURE_HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("signature-hash-algorithm")
            .displayName("Signature Hash Function")
            .description("The hash function used when signing data.")
            .allowableValues(getSignatureHashAllowableValues())
            .defaultValue(getSignatureHashDefaultValue())
            .build();

    public static final PropertyDescriptor SIGNATURE_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("signature-attribute")
            .displayName("Signature Attribute")
            .description("The name of the FlowFile Attribute for the signature to write during Sign operations and to read during Verify operations.")
            .defaultValue(DEFAULT_SIGNATURE_ATTRIBUTE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected final KeyCache<PGPPublicKeys> publicKeyCache = new KeyCache<>(); // Operator instances have a public key cache
    protected final KeyCache<PGPSecretKeys> secretKeyCache = new KeyCache<>(); // and a private key cache


    public static String getSignatureHashDefaultValue() {
        return String.valueOf(PGPUtil.SHA256);
    }


    // Values match integer values in org.bouncycastle.bcpg.HashAlgorithmTags
    static AllowableValue[] getSignatureHashAllowableValues() {
        return new AllowableValue[]{
                new AllowableValue("1", "MD5"),
                new AllowableValue("2", "SHA1"),
                new AllowableValue("6", "TIGER 192"),
                new AllowableValue("8", "SHA 256"),
                new AllowableValue("9", "SHA 384"),
                new AllowableValue("10", "SHA 512"),
        };
    }

    // Values match integer values in org.bouncycastle.bcpg.SymmetricKeyAlgorithmTags
    static AllowableValue[] getCipherAllowableValues() {
        return new AllowableValue[]{
                // 0 - NULL not supported
                new AllowableValue("1", "IDEA"),
                new AllowableValue("2", "TRIPLE DES"),
                new AllowableValue("3", "CAST5"),
                new AllowableValue("4", "BLOWFISH"),
                new AllowableValue("6", "DES"),
                // 6 - SAFER not supported
                new AllowableValue("7", "AES 128"),
                new AllowableValue("8", "AES 192"),
                new AllowableValue("9", "AES 256"),
                new AllowableValue("10", "TWOFISH"),
                new AllowableValue("11", "CAMELLIA 128"),
                new AllowableValue("12", "CAMELLIA 192"),
                new AllowableValue("13", "CAMELLIA 256")};
    }

    public static String getCipherDefaultValue() {
        return String.valueOf(PGPEncryptedData.AES_128);
    }

    public char[] getPBEPassphrase(PropertyContext context) {
        if (context.getProperty(PBE_PASSPHRASE).isSet()) {
            return context.getProperty(PBE_PASSPHRASE).getValue().toCharArray();
        }
        return null;
    }

    public InputStream getSignature(PropertyContext context, FlowFile flowFile) throws PGPException {
        if (!context.getProperty(SIGNATURE_ATTRIBUTE).isSet()) {
            throw new PGPException("Signature attribute not set.");
        }

        final String signature = flowFile.getAttribute(context.getProperty(SIGNATURE_ATTRIBUTE).getValue());
        try {
            return new ByteArrayInputStream(Hex.decodeHex(signature));
        } catch (final DecoderException e) {
            throw new PGPException("Unable to decode signature.", e);
        }
    }


    /**
     * Creates map of public keys from the input stream.
     *
     * @param input public key or key ring stream
     * @param hash value to use as cache key
     * @return public keys
     */
    PGPPublicKeys readPublicKeys(InputStream input, Integer hash) {
         if (hash != null && publicKeyCache.containsKey(hash)) {
            return publicKeyCache.get(hash);
        }

        final PGPPublicKeys keys = new PGPPublicKeys();
        JcaPGPPublicKeyRingCollection rings;

        try {
            InputStream decoderStream = PGPUtil.getDecoderStream(input);
            rings = new JcaPGPPublicKeyRingCollection(decoderStream);
            input.close();
        } catch (final IOException | PGPException ignored) {
            return null;
        }

        for (final PGPPublicKeyRing ring : rings) {
            for (final PGPPublicKey key : ring) {
                for (final Iterator<String> it = key.getUserIDs(); it.hasNext(); ) {
                    keys.put(it.next(), key);
                }
            }
        }

        if (hash != null && keys.size() > 0) publicKeyCache.putMiss(hash, keys);
        return keys;
    }

    /**
     * Creates a list of public keys from the input stream.  When called with a lone {@link InputStream}, this method
     * will not cache the resulting keys.
     *
     * @param input public key or key ring stream
     * @return public keys
     */
    public PGPPublicKeys readPublicKeys(InputStream input) {
        return readPublicKeys(input, null);
    }

    /**
     * Creates a list of public keys from the given String.
     *
     * @param source public key text
     * @return pubic keys
     */
    public PGPPublicKeys readPublicKeys(String source) {
        return readPublicKeys(new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8)), source.hashCode());
    }

    /**
     * Creates a list of public keys from the given {@link File}.
     *
     * @param source public key File
     * @return public keys
     */
    public PGPPublicKeys readPublicKeys(File source) throws FileNotFoundException {
        return readPublicKeys(new FileInputStream(source), source.getAbsoluteFile().hashCode());
    }

    /**
     * Creates a list of secret keys from the input stream.
     *
     * @param input secret key or key ring stream
     * @return secret keys
     */
    PGPSecretKeys readSecretKeys(InputStream input, Integer hash) {
        if (hash != null && secretKeyCache.containsKey(hash)) {
            return secretKeyCache.get(hash);
        }

        final PGPSecretKeys keys = new PGPSecretKeys();
        final KeyFingerPrintCalculator calc = new BcKeyFingerprintCalculator();
        PGPSecretKeyRingCollection rings;

        try {
            rings = new PGPSecretKeyRingCollection(PGPUtil.getDecoderStream(input), calc);
            input.close();
        } catch (final IOException | PGPException ignored) {
            return null;
        }

        for (final PGPSecretKeyRing ring : rings) {
            for (final PGPSecretKey secretKey : ring) {
                for (final Iterator<String> it = secretKey.getUserIDs(); it.hasNext(); ) {
                    final String userID = it.next();
                    keys.put(userID, secretKey);
                }
            }
        }

        if (hash != null && keys.size() > 0) secretKeyCache.putMiss(hash, keys);
        return keys;
    }

    /**
     * Creates a list of secret keys from the input stream.  When called with a lone {@link InputStream}, this method
     * does not cache the resulting keys.
     *
     * @param input secret key or key ring stream
     * @return secret keys
     */
    public PGPSecretKeys readSecretKeys(InputStream input) {
        return readSecretKeys(input, null);
    }

    /**
     * Creates a list of secret keys from the input stream.  The hash code of the source string is used as the cache
     * key for the resulting secret key ring.
     *
     * @param source secret key text
     * @return secret keys
     */

    public PGPSecretKeys readSecretKeys(String source) {
        return readSecretKeys(new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8)), source.hashCode());
    }

    /**
     * Creates a list of secret keys from the input stream.  The hash code of the filename is used as the cache key for
     * the resulting secret key ring.
     *
     * @param source secret key File
     * @return secret keys
     */
    public PGPSecretKeys readSecretKeys(File source) throws FileNotFoundException {
        return readSecretKeys(new FileInputStream(source), source.getAbsolutePath().hashCode());
    }


    /**
     * Read from an {@link InputStream} and write an encrypted representation to an {@link OutputStream}.
     *
     * @param input plain data
     * @param output to receive encrypted data
     * @param options used to configure encryption operation
     */
    public void encrypt(InputStream input, OutputStream output, EncryptOptions options) throws IOException, PGPException {
        final int bufferSize = 65536;
        final PGPEncryptedDataGenerator generator = options.getDataGenerator();
        final boolean armor = options.getArmor();
        final OutputStream out = armor ? new ArmoredOutputStream(output) : output;

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
        try (InputStream decodedInput = PGPUtil.getDecoderStream(input)) {
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
        JcaPGPObjectFactory signatureFactory = new JcaPGPObjectFactory(PGPUtil.getDecoderStream(signature));
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
     * @return encrypt options
     */
    @Override
    public EncryptOptions optionsForEncrypt(PropertyContext context) {
        final int algorithm = context.getProperty(ENCRYPT_ALGORITHM).asInteger();
        final boolean armor = context.getProperty(ENCRYPT_ENCODING).getValue().equals("1");

        PGPEncryptedDataGenerator generator = new PGPEncryptedDataGenerator(new JcePGPDataEncryptorBuilder(algorithm)
                .setWithIntegrityPacket(true)
                .setProvider("BC"));
        char[] passphrase = getPBEPassphrase(context);
        if (passphrase != null && passphrase.length != 0) {
            generator.addMethod(new JcePBEKeyEncryptionMethodGenerator(passphrase).setProvider("BC"));
        } else {
            generator.addMethod(new JcePublicKeyKeyEncryptionMethodGenerator(getPublicKey(context)).setProvider("BC"));
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
     * @param context work in progress change this
     */
    @Override
    public DecryptOptions optionsForDecrypt(PropertyContext context) {
        PGPDigestCalculatorProvider calc = null;
        try {
            calc = new JcaPGPDigestCalculatorProviderBuilder().setProvider("BC").build();
        } catch (PGPException e) {
            calc = new BcPGPDigestCalculatorProvider();
        }

        char[] passphrase = getPBEPassphrase(context);
        if (passphrase != null && passphrase.length != 0) {
            PBEDataDecryptorFactory factory = new JcePBEDataDecryptorFactoryBuilder(calc).setProvider("BC").build(passphrase);
            return packet -> ((PGPPBEEncryptedData) packet).getDataStream(factory);
        } else {
            PublicKeyDataDecryptorFactory factory = new JcePublicKeyDataDecryptorFactoryBuilder().setProvider("BC").build(getPrivateKey(context));
            return packet -> ((PGPPublicKeyEncryptedData) packet).getDataStream(factory);
        }
    }

    /**
     * Generate a {@link SignOptions} instance for a sign operation.
     *
     * @param context signature algorithm identifier, see {@link HashAlgorithmTags}
     * @return sign options
     */
    @Override
    public SignOptions optionsForSign(PropertyContext context) {
        int algorithm = context.getProperty(SIGNATURE_HASH_ALGORITHM).asInteger();
        PGPPrivateKey privateKey = getPrivateKey(context);

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
     * @param context work in progress change this
     */
    @Override
    public VerifyOptions optionsForVerify(PropertyContext context) {
        PGPPublicKey publicKey = getPublicKey(context);
        return new VerifyOptions() {
            @Override
            public PGPPublicKey getPublicKey() {
                return publicKey;
            }
        };
    }


    public PGPPublicKey getPublicKey(PropertyContext context) {
        PGPPublicKeys keys = null;

        if (context.getProperty(PUBLIC_KEYRING_TEXT).isSet()) {
            keys = readPublicKeys(context.getProperty(PUBLIC_KEYRING_TEXT).getValue());

        } else if (context.getProperty(PUBLIC_KEYRING_FILE).isSet()) {
            try {
                keys = readPublicKeys(new File(context.getProperty(PUBLIC_KEYRING_FILE).getValue()));
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
        } else if (keys.size() == 1) {
            return keys.values().iterator().next();
        } else {
            logger.debug("Unable to find public key from context.");
            return null;
        }
    }


    public PGPPrivateKey getPrivateKey(PropertyContext context) {
        String secretKeyUserId = null;
        PGPSecretKeys keys = null;
        PBESecretKeyDecryptor decryptor = null;

        if (context.getProperty(SECRET_KEY_USER_ID).isSet()) {
            secretKeyUserId = context.getProperty(SECRET_KEY_USER_ID).getValue();
        }

        if (context.getProperty(PRIVATE_KEY_PASSPHRASE).isSet()) {
            try {
                decryptor = new JcePBESecretKeyDecryptorBuilder().setProvider("BC").build(context.getProperty(PRIVATE_KEY_PASSPHRASE).getValue().toCharArray());
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

        // Return the specific key when it's given:
        if (secretKeyUserId != null) {
            try {
                return keys.getSecretKey(secretKeyUserId).extractPrivateKey(decryptor);
            } catch (final PGPException e) {
                logger.debug("Unable to extract private key from secret key: " + e);
                return null;
            }
        }

        // Because PGP keys are often transmitted as a keyring of one key, we handle that case specifically:
        if (keys.size() == 1) {
            try {
                return keys.values().iterator().next().extractPrivateKey(decryptor);
            } catch (final PGPException e) {
                logger.debug("Unable to extract private key from secret keyring (of one key): " + e);
                return null;
            }
        }

        // More than one key and no user id given:
        logger.debug("Unable to find private key from context.");
        return null;
    }

    public void clearCache() {
        this.publicKeyCache.clear();
        this.secretKeyCache.clear();
    }

    public boolean isContextForEncryptValid(PropertyContext context) {
        PGPPublicKey key = getPublicKey(context);
        return (key != null && key.isEncryptionKey()) || (getPBEPassphrase(context) != null);
    }

    public boolean isContextForDecryptValid(PropertyContext context) {
        return (getPrivateKey(context) != null) || (getPBEPassphrase(context) != null);
    }

    public boolean isContextForSignValid(PropertyContext context) {
        return getPrivateKey(context) != null;
    }

    public boolean isContextForVerifyValid(PropertyContext context) {
        final PGPPublicKey key = getPublicKey(context);
        return key != null && key.isEncryptionKey();
    }


    public boolean isContextForEncrypt(PropertyContext context) {
        return context.getProperty(PBE_PASSPHRASE).isSet()
                || context.getProperty(PUBLIC_KEYRING_TEXT).isSet()
                || context.getProperty(PUBLIC_KEYRING_FILE).isSet();
    }


    public boolean isContextForDecrypt(PropertyContext context) {
        return context.getProperty(PBE_PASSPHRASE).isSet()
                || context.getProperty(SECRET_KEYRING_TEXT).isSet()
                || context.getProperty(SECRET_KEYRING_FILE).isSet();
    }


    public boolean isContextForSign(PropertyContext context) {
        return context.getProperty(SECRET_KEYRING_TEXT).isSet()
                || context.getProperty(SECRET_KEYRING_FILE).isSet();
    }


    public boolean isContextForVerify(PropertyContext context) {
        return context.getProperty(PUBLIC_KEYRING_TEXT).isSet()
                || context.getProperty(PUBLIC_KEYRING_FILE).isSet();
    }


    protected class KeyCache<T> {
        private final Map<Integer, T> cache = new HashMap<>();
        private int hit = 0;
        private int miss = 0;

        T get(Integer id) {
            T key = cache.get(id);
            if (key != null) {
                hit++;
            } else {
                miss++;
            }
            return key;
        }

        boolean containsKey(Integer id) {
            return cache.containsKey(id);
        }

        void putMiss(Integer id, T keys) {
            miss++;
            cache.put(id, keys);
        }

        int hits() {
            return hit;
        }

        int misses() {
            return miss;
        }

        public void clear() {
            this.cache.clear();
            this.hit = 0;
            this.miss = 0;
        }
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
