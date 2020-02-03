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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.operator.PBESecretKeyDecryptor;
import org.bouncycastle.openpgp.operator.jcajce.JcePBESecretKeyDecryptorBuilder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@CapabilityDescription("Defines key materials for PGP processors.")
@Tags({"pgp", "gpg", "encryption", "credentials", "provider"})
public class PGPKeyMaterialControllerService extends AbstractControllerService implements PGPKeyMaterialService {
    final static String CONTROLLER_NAME = "PGP Key Material Controller Service";

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

    public static final PropertyDescriptor PRIVATE_KEY_PASS_PHRASE = new PropertyDescriptor.Builder()
            .name("private-key-passphrase")
            .displayName("Private Key Pass Phrase")
            .description("This is the pass-phrase for the specified secret key.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor PBE_PASS_PHRASE = new PropertyDescriptor.Builder()
            .name("pbe-pass-phrase")
            .displayName("Encryption Pass Phrase")
            .description("This is the pass phrase for password-based encryption and decryption.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(true)
            .build();


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PUBLIC_KEYRING_FILE);
        properties.add(PUBLIC_KEYRING_TEXT);
        properties.add(PUBLIC_KEY_USER_ID);
        properties.add(SECRET_KEYRING_FILE);
        properties.add(SECRET_KEYRING_TEXT);
        properties.add(SECRET_KEY_USER_ID);
        properties.add(PRIVATE_KEY_PASS_PHRASE);
        properties.add(PBE_PASS_PHRASE);
        return properties;
    }


    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        boolean canEncrypt = validateForEncrypt(validationContext).isEmpty();
        if (canEncrypt) return Collections.emptySet();

        boolean canDecrypt = validateForDecrypt(validationContext).isEmpty();
        if (canDecrypt) return Collections.emptySet();

        boolean canSign = validateForSign(validationContext).isEmpty();
        if (canSign) return Collections.emptySet();

        boolean canVerify = validateForVerify(validationContext).isEmpty();
        if (canVerify) return Collections.emptySet();

        final List<ValidationResult> problems = new ArrayList<>();
        problems.add(new ValidationResult.Builder()
                .subject(CONTROLLER_NAME)
                .valid(false)
                .explanation("Controller not configured for any operation.")
                .build());
        return problems;
    }


    @Override
    public PGPPublicKey getPublicKey(PropertyContext context) {
        List<PGPPublicKey> keys = null;

        if (context.getProperty(PUBLIC_KEYRING_TEXT).isSet()) {
            final String keyText = context.getProperty(PUBLIC_KEYRING_TEXT).getValue();
            keys = StaticKeyMaterialProvider.getPublicKeys(new ByteArrayInputStream(keyText.getBytes(StandardCharsets.UTF_8)));

        } else if (context.getProperty(PUBLIC_KEYRING_FILE).isSet()) {
            final String keyFile = context.getProperty(PUBLIC_KEYRING_FILE).getValue();
            try {
                keys = StaticKeyMaterialProvider.getPublicKeys(new FileInputStream(new File(keyFile)));
            } catch (FileNotFoundException ignored) {
            }
        }

        if (keys == null || keys.size() == 0)
            return null;

        if (context.getProperty(PUBLIC_KEY_USER_ID).isSet()) {
            return StaticKeyMaterialProvider.getPublicKeyFromUser(keys, context.getProperty(PUBLIC_KEY_USER_ID).getValue());
        }

        for (PGPPublicKey key : keys) {
            if (key.isEncryptionKey())
                    return key;
        }
        return null;
    }

    @Override
    public PGPPrivateKey getPrivateKey(PropertyContext context) {
        String secretKeyUserId = null;
        char[] privateKeyPassPhrase = null;
        List<PGPSecretKey> keys = null;
        PBESecretKeyDecryptor decryptor = null;

        if (context.getProperty(SECRET_KEY_USER_ID).isSet()) {
            secretKeyUserId = context.getProperty(SECRET_KEY_USER_ID).getValue();
        }

        if (context.getProperty(PRIVATE_KEY_PASS_PHRASE).isSet()) {
            privateKeyPassPhrase = context.getProperty(PRIVATE_KEY_PASS_PHRASE).getValue().toCharArray();

            try {
                decryptor = new JcePBESecretKeyDecryptorBuilder().setProvider("BC").build(privateKeyPassPhrase);
            } catch (PGPException e) {
                return null;
            }
        }

        if (context.getProperty(SECRET_KEYRING_TEXT).isSet()) {
            final String secretKeyText = context.getProperty(SECRET_KEYRING_TEXT).getValue();
            keys = StaticKeyMaterialProvider.getSecretKeys(new ByteArrayInputStream(secretKeyText.getBytes(StandardCharsets.UTF_8)));

        } else  if (context.getProperty(SECRET_KEYRING_FILE).isSet()) {
            try {
                keys = StaticKeyMaterialProvider.getSecretKeys(new FileInputStream(new File(context.getProperty(SECRET_KEYRING_FILE).getValue())));
            } catch (FileNotFoundException e) {
                return null;
            }
        }

        if (keys == null || keys.size() == 0)
            return null;

        if (secretKeyUserId != null) {
            try {
                return StaticKeyMaterialProvider.getSecretKeyFromUser(keys, secretKeyUserId).extractPrivateKey(decryptor);
            } catch (PGPException e) {
                return null;
            }
        }

        for (PGPSecretKey key : keys) {
            if (!key.isPrivateKeyEmpty()) {
                try {
                    return key.extractPrivateKey(decryptor);
                } catch (final Exception ignored) {
                    // pass
                }
            }
        }
        return null; // no private key available from the given config
    }

    @Override
    public char[] getPBEPassPhrase(PropertyContext context) {
        if (context.getProperty(PBE_PASS_PHRASE).isSet()) {
            return context.getProperty(PBE_PASS_PHRASE).getValue().toCharArray();
        }
        return null;
    }

    @Override
    public PGPPublicKey getPublicKey() {
        return getPublicKey(getConfigurationContext());
    }

    @Override
    public PGPPrivateKey getPrivateKey() {
        return getPrivateKey(getConfigurationContext());
    }

    @Override
    public char[] getPBEPassPhrase() {
        return getPBEPassPhrase(getConfigurationContext());
    }

    @Override
    public Collection<ValidationResult> validateForEncrypt(PropertyContext context) {
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
                .explanation("Controller requires a public key or PBE pass-phrase for Encrypt operations.")
                .build());
        return problems;
    }

    @Override
    public Collection<ValidationResult> validateForDecrypt(PropertyContext context) {
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
                .explanation("Controller requires a secret key or PBE pass-phrase for Decrypt operations.")
                .build());
        return problems;
    }

    @Override
    public Collection<ValidationResult> validateForSign(PropertyContext context) {
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

    @Override
    public Collection<ValidationResult> validateForVerify(PropertyContext context) {
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

}
