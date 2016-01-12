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
package org.apache.nifi.processors.standard;

import java.security.Security;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.OpenPGPKeyBasedEncryptor;
import org.apache.nifi.processors.standard.util.OpenPGPPasswordBasedEncryptor;
import org.apache.nifi.processors.standard.util.PasswordBasedEncryptor;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.util.StopWatch;
import org.bouncycastle.jce.provider.BouncyCastleProvider;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"encryption", "decryption", "password", "JCE", "OpenPGP", "PGP", "GPG"})
@CapabilityDescription("Encrypts or Decrypts a FlowFile using either symmetric encryption with a password and randomly generated salt, or asymmetric encryption using a public and secret key.")
public class EncryptContent extends AbstractProcessor {

    public static final String ENCRYPT_MODE = "Encrypt";
    public static final String DECRYPT_MODE = "Decrypt";

    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
        .name("Mode")
        .description("Specifies whether the content should be encrypted or decrypted")
        .required(true)
        .allowableValues(ENCRYPT_MODE, DECRYPT_MODE)
        .defaultValue(ENCRYPT_MODE)
        .build();
    public static final PropertyDescriptor KEY_DERIVATION_FUNCTION = new PropertyDescriptor.Builder()
        .name("key-derivation-function")
        .displayName("Key Derivation Function")
        .description("Specifies the key derivation function to generate the key from the password (and salt)")
        .required(true)
        .allowableValues(KeyDerivationFunction.values())
        .defaultValue(KeyDerivationFunction.NIFI_LEGACY.name())
        .build();
    public static final PropertyDescriptor ENCRYPTION_ALGORITHM = new PropertyDescriptor.Builder()
        .name("Encryption Algorithm")
        .description("The Encryption Algorithm to use")
        .required(true)
        .allowableValues(EncryptionMethod.values())
        .defaultValue(EncryptionMethod.MD5_128AES.name())
        .build();
    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
        .name("Password")
        .description("The Password to use for encrypting or decrypting the data")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(true)
        .build();
    public static final PropertyDescriptor PUBLIC_KEYRING = new PropertyDescriptor.Builder()
        .name("public-keyring-file")
        .displayName("Public Keyring File")
        .description("In a PGP encrypt mode, this keyring contains the public key of the recipient")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor PUBLIC_KEY_USERID = new PropertyDescriptor.Builder()
        .name("public-key-user-id")
        .displayName("Public Key User Id")
        .description("In a PGP encrypt mode, this user id of the recipient")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor PRIVATE_KEYRING = new PropertyDescriptor.Builder()
        .name("private-keyring-file")
        .displayName("Private Keyring File")
        .description("In a PGP decrypt mode, this keyring contains the private key of the recipient")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    public static final PropertyDescriptor PRIVATE_KEYRING_PASSPHRASE = new PropertyDescriptor.Builder()
        .name("private-keyring-passphrase")
        .displayName("Private Keyring Passphrase")
        .description("In a PGP decrypt mode, this is the private keyring passphrase")
        .required(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .sensitive(true)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
        .description("Any FlowFile that is successfully encrypted or decrypted will be routed to success").build();
    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
        .description("Any FlowFile that cannot be encrypted or decrypted will be routed to failure").build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    static {
        // add BouncyCastle encryption providers
        Security.addProvider(new BouncyCastleProvider());
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MODE);
        properties.add(KEY_DERIVATION_FUNCTION);
        properties.add(ENCRYPTION_ALGORITHM);
        properties.add(PASSWORD);
        properties.add(PUBLIC_KEYRING);
        properties.add(PUBLIC_KEY_USERID);
        properties.add(PRIVATE_KEYRING);
        properties.add(PRIVATE_KEYRING_PASSPHRASE);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    public static boolean isPGPAlgorithm(final String algorithm) {
        return algorithm.startsWith("PGP");
    }

    public static boolean isPGPArmoredAlgorithm(final String algorithm) {
        return isPGPAlgorithm(algorithm) && algorithm.endsWith("ASCII-ARMOR");
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));
        final String methodValue = context.getProperty(ENCRYPTION_ALGORITHM).getValue();
        final EncryptionMethod method = EncryptionMethod.valueOf(methodValue);
        final String algorithm = method.getAlgorithm();
        final String password = context.getProperty(PASSWORD).getValue();
        final String kdf = context.getProperty(KEY_DERIVATION_FUNCTION).getValue();
        if (isPGPAlgorithm(algorithm)) {
            if (password == null) {
                final boolean encrypt = context.getProperty(MODE).getValue().equalsIgnoreCase(ENCRYPT_MODE);
                if (encrypt) {
                    // need both public-keyring-file and public-key-user-id set
                    final String publicKeyring = context.getProperty(PUBLIC_KEYRING).getValue();
                    final String publicUserId = context.getProperty(PUBLIC_KEY_USERID).getValue();
                    if (publicKeyring == null || publicUserId == null) {
                        validationResults.add(new ValidationResult.Builder().subject(PUBLIC_KEYRING.getDisplayName())
                            .explanation(algorithm + " encryption without a " + PASSWORD.getDisplayName() + " requires both "
                                + PUBLIC_KEYRING.getDisplayName() + " and " + PUBLIC_KEY_USERID.getDisplayName())
                            .build());
                    } else {
                        // verify the public keyring contains the user id
                        try {
                            if (OpenPGPKeyBasedEncryptor.getPublicKey(publicUserId, publicKeyring) == null) {
                                validationResults.add(new ValidationResult.Builder().subject(PUBLIC_KEYRING.getDisplayName())
                                    .explanation(PUBLIC_KEYRING.getDisplayName() + " " + publicKeyring
                                        + " does not contain user id " + publicUserId)
                                    .build());
                            }
                        } catch (final Exception e) {
                            validationResults.add(new ValidationResult.Builder().subject(PUBLIC_KEYRING.getDisplayName())
                                .explanation("Invalid " + PUBLIC_KEYRING.getDisplayName() + " " + publicKeyring
                                    + " because " + e.toString())
                                .build());
                        }
                    }
                } else {
                    // need both private-keyring-file and private-keyring-passphrase set
                    final String privateKeyring = context.getProperty(PRIVATE_KEYRING).getValue();
                    final String keyringPassphrase = context.getProperty(PRIVATE_KEYRING_PASSPHRASE).getValue();
                    if (privateKeyring == null || keyringPassphrase == null) {
                        validationResults.add(new ValidationResult.Builder().subject(PRIVATE_KEYRING.getName())
                            .explanation(algorithm + " decryption without a " + PASSWORD.getDisplayName() + " requires both "
                                + PRIVATE_KEYRING.getDisplayName() + " and " + PRIVATE_KEYRING_PASSPHRASE.getDisplayName())
                            .build());
                    } else {
                        final String providerName = EncryptionMethod.valueOf(methodValue).getProvider();
                        // verify the passphrase works on the private keyring
                        try {
                            if (!OpenPGPKeyBasedEncryptor.validateKeyring(providerName, privateKeyring, keyringPassphrase.toCharArray())) {
                                validationResults.add(new ValidationResult.Builder().subject(PRIVATE_KEYRING.getDisplayName())
                                    .explanation(PRIVATE_KEYRING.getDisplayName() + " " + privateKeyring
                                        + " could not be opened with the provided " + PRIVATE_KEYRING_PASSPHRASE.getDisplayName())
                                    .build());
                            }
                        } catch (final Exception e) {
                            validationResults.add(new ValidationResult.Builder().subject(PRIVATE_KEYRING.getDisplayName())
                                .explanation("Invalid " + PRIVATE_KEYRING.getDisplayName() + " " + privateKeyring
                                    + " because " + e.toString())
                                .build());
                        }
                    }
                }
            }
        } else { // PBE
            if (!PasswordBasedEncryptor.supportsUnlimitedStrength()) {
                if (method.isUnlimitedStrength()) {
                    validationResults.add(new ValidationResult.Builder().subject(ENCRYPTION_ALGORITHM.getName())
                        .explanation(methodValue + " (" + algorithm + ") is not supported by this JVM due to lacking JCE Unlimited " +
                            "Strength Jurisdiction Policy files.").build());
                }
            }
            int allowedKeyLength = PasswordBasedEncryptor.getMaxAllowedKeyLength(ENCRYPTION_ALGORITHM.getName());

            if (StringUtils.isEmpty(password)) {
                validationResults.add(new ValidationResult.Builder().subject(PASSWORD.getName())
                    .explanation(PASSWORD.getDisplayName() + " is required when using algorithm " + algorithm).build());
            } else {
                if (password.getBytes().length * 8 > allowedKeyLength) {
                    validationResults.add(new ValidationResult.Builder().subject(PASSWORD.getName())
                        .explanation("Password length greater than " + allowedKeyLength + " bits is not supported by this JVM" +
                            " due to lacking JCE Unlimited Strength Jurisdiction Policy files.").build());
                }
            }

            // Perform some analysis on the selected encryption algorithm to ensure the JVM can support it and the associated key

            if (StringUtils.isEmpty(kdf)) {
                validationResults.add(new ValidationResult.Builder().subject(KEY_DERIVATION_FUNCTION.getName())
                    .explanation(KEY_DERIVATION_FUNCTION.getDisplayName() + " is required when using algorithm " + algorithm).build());
            }
        }
        return validationResults;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ProcessorLog logger = getLogger();
        final String method = context.getProperty(ENCRYPTION_ALGORITHM).getValue();
        final EncryptionMethod encryptionMethod = EncryptionMethod.valueOf(method);
        final String providerName = encryptionMethod.getProvider();
        final String algorithm = encryptionMethod.getAlgorithm();
        final String password = context.getProperty(PASSWORD).getValue();
        final KeyDerivationFunction kdf = KeyDerivationFunction.valueOf(context.getProperty(KEY_DERIVATION_FUNCTION).getValue());
        final boolean encrypt = context.getProperty(MODE).getValue().equalsIgnoreCase(ENCRYPT_MODE);

        Encryptor encryptor;
        StreamCallback callback;
        try {
            if (isPGPAlgorithm(algorithm)) {
                final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());
                final String publicKeyring = context.getProperty(PUBLIC_KEYRING).getValue();
                final String privateKeyring = context.getProperty(PRIVATE_KEYRING).getValue();
                if (encrypt && publicKeyring != null) {
                    final String publicUserId = context.getProperty(PUBLIC_KEY_USERID).getValue();
                    encryptor = new OpenPGPKeyBasedEncryptor(algorithm, providerName, publicKeyring, publicUserId, null, filename);
                } else if (!encrypt && privateKeyring != null) {
                    final char[] keyringPassphrase = context.getProperty(PRIVATE_KEYRING_PASSPHRASE).getValue().toCharArray();
                    encryptor = new OpenPGPKeyBasedEncryptor(algorithm, providerName, privateKeyring, null, keyringPassphrase,
                        filename);
                } else {
                    final char[] passphrase = Normalizer.normalize(password, Normalizer.Form.NFC).toCharArray();
                    encryptor = new OpenPGPPasswordBasedEncryptor(algorithm, providerName, passphrase, filename);
                }
            } else { // PBE
                final char[] passphrase = Normalizer.normalize(password, Normalizer.Form.NFC).toCharArray();
                encryptor = new PasswordBasedEncryptor(algorithm, providerName, passphrase, kdf);
            }

            if (encrypt) {
                callback = encryptor.getEncryptionCallback();
            } else {
                callback = encryptor.getDecryptionCallback();
            }

        } catch (final Exception e) {
            logger.error("Failed to initialize {}cryption algorithm because - ", new Object[]{encrypt ? "en" : "de", e});
            session.rollback();
            context.yield();
            return;
        }

        try {
            final StopWatch stopWatch = new StopWatch(true);
            flowFile = session.write(flowFile, callback);
            logger.info("successfully {}crypted {}", new Object[]{encrypt ? "en" : "de", flowFile});
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final ProcessException e) {
            logger.error("Cannot {}crypt {} - ", new Object[]{encrypt ? "en" : "de", flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
    }

    public interface Encryptor {
        StreamCallback getEncryptionCallback() throws Exception;

        StreamCallback getDecryptionCallback() throws Exception;
    }

}