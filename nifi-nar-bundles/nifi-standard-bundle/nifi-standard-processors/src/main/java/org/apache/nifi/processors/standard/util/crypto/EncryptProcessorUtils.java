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
package org.apache.nifi.processors.standard.util.crypto;


import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class EncryptProcessorUtils {


    public static final String WEAK_CRYPTO_ALLOWED_NAME = "allowed";
    public static final String WEAK_CRYPTO_NOT_ALLOWED_NAME = "not-allowed";
    public static final String PUBLIC_KEYRING = "Public Keyring File";
    public static final String PUBLIC_KEY_USERID = "Public Key User Id";
    public static final String PRIVATE_KEYRING = "Private Keyring File";
    public static final String PRIVATE_KEYRING_PASSPHRASE = "Private Keyring Passphrase";
    public static final String RAW_KEY_HEX= "Raw Key (hexadecimal)";
    public static final String PASSWORD = "Password";
    public static final String ENCRYPTION_ALGORITHM = "Encryption Algorithm";
    public static final String KEY_DERIVATION_FUNCTION = "Key Derivation Function";


    public static boolean isPGPAlgorithm(final String algorithm) {
        return algorithm.startsWith("PGP");
    }

    public static boolean isPGPArmoredAlgorithm(final String algorithm) {
        return isPGPAlgorithm(algorithm) && algorithm.endsWith("ASCII-ARMOR");
    }


    public static AllowableValue[] buildKeyDerivationFunctionAllowableValues() {
        final KeyDerivationFunction[] keyDerivationFunctions = KeyDerivationFunction.values();
        List<AllowableValue> allowableValues = new ArrayList<>(keyDerivationFunctions.length);
        for (KeyDerivationFunction kdf : keyDerivationFunctions) {
            allowableValues.add(new AllowableValue(kdf.name(), kdf.getName(), kdf.getDescription()));
        }

        return allowableValues.toArray(new AllowableValue[0]);
    }

    public static AllowableValue[] buildEncryptionMethodAllowableValues() {
        final EncryptionMethod[] encryptionMethods = EncryptionMethod.values();
        List<AllowableValue> allowableValues = new ArrayList<>(encryptionMethods.length);
        for (EncryptionMethod em : encryptionMethods) {
            allowableValues.add(new AllowableValue(em.name(), em.name(), em.toString()));
        }

        return allowableValues.toArray(new AllowableValue[0]);
    }

    public static AllowableValue[] buildWeakCryptoAllowableValues() {
        List<AllowableValue> allowableValues = new ArrayList<>();
        allowableValues.add(new AllowableValue(WEAK_CRYPTO_ALLOWED_NAME, "Allowed", "Operation will not be blocked and no alerts will be presented " +
                "when unsafe combinations of encryption algorithms and passwords are provided"));
        allowableValues.add(buildDefaultWeakCryptoAllowableValue());
        return allowableValues.toArray(new AllowableValue[0]);
    }

    public static AllowableValue buildDefaultWeakCryptoAllowableValue() {
        return new AllowableValue(WEAK_CRYPTO_NOT_ALLOWED_NAME, "Not Allowed", "When set, operation will be blocked and alerts will be presented to the user " +
                "if unsafe combinations of encryption algorithms and passwords are provided on a JVM with limited strength crypto. To fix this, see the Admin Guide.");
    }

    public static List<ValidationResult> validatePGP(EncryptionMethod encryptionMethod, String password, boolean encrypt, String publicKeyring, String publicUserId, String privateKeyring,
                                              String privateKeyringPassphrase) {
        List<ValidationResult> validationResults = new ArrayList<>();

        if (password == null) {
            if (encrypt) {
                // If encrypting without a password, require both public-keyring-file and public-key-user-id
                if (publicKeyring == null || publicUserId == null) {
                    validationResults.add(new ValidationResult.Builder().subject(PUBLIC_KEYRING)
                            .explanation(encryptionMethod.getAlgorithm() + " encryption without a " + PASSWORD + " requires both "
                                    + PUBLIC_KEYRING + " and " + PUBLIC_KEY_USERID)
                            .build());
                } else {
                    // Verify the public keyring contains the user id
                    try {
                        if (OpenPGPKeyBasedEncryptor.getPublicKey(publicUserId, publicKeyring) == null) {
                            validationResults.add(new ValidationResult.Builder().subject(PUBLIC_KEYRING)
                                    .explanation(PUBLIC_KEYRING + " " + publicKeyring
                                            + " does not contain user id " + publicUserId)
                                    .build());
                        }
                    } catch (final Exception e) {
                        validationResults.add(new ValidationResult.Builder().subject(PUBLIC_KEYRING)
                                .explanation("Invalid " + PUBLIC_KEYRING + " " + publicKeyring
                                        + " because " + e.toString())
                                .build());
                    }
                }
            } else { // Decrypt
                // Require both private-keyring-file and private-keyring-passphrase
                if (privateKeyring == null || privateKeyringPassphrase == null) {
                    validationResults.add(new ValidationResult.Builder().subject(PRIVATE_KEYRING)
                            .explanation(encryptionMethod.getAlgorithm() + " decryption without a " + PASSWORD + " requires both "
                                    + PRIVATE_KEYRING + " and " + PRIVATE_KEYRING_PASSPHRASE)
                            .build());
                } else {
                    final String providerName = encryptionMethod.getProvider();
                    // Verify the passphrase works on the private keyring
                    try {
                        if (!OpenPGPKeyBasedEncryptor.validateKeyring(providerName, privateKeyring, privateKeyringPassphrase.toCharArray())) {
                            validationResults.add(new ValidationResult.Builder().subject(PRIVATE_KEYRING)
                                    .explanation(PRIVATE_KEYRING + " " + privateKeyring
                                            + " could not be opened with the provided " + PRIVATE_KEYRING_PASSPHRASE)
                                    .build());
                        }
                    } catch (final Exception e) {
                        validationResults.add(new ValidationResult.Builder().subject(PRIVATE_KEYRING)
                                .explanation("Invalid " + PRIVATE_KEYRING + " " + privateKeyring
                                        + " because " + e.toString())
                                .build());
                    }
                }
            }
        }

        return validationResults;
    }

    public static List<ValidationResult> validatePBE(EncryptionMethod encryptionMethod, KeyDerivationFunction kdf, String password, boolean allowWeakCrypto) {
        List<ValidationResult> validationResults = new ArrayList<>();
        boolean limitedStrengthCrypto = !PasswordBasedEncryptor.supportsUnlimitedStrength();

        // Password required (short circuits validation because other conditions depend on password presence)
        if (StringUtils.isEmpty(password)) {
            validationResults.add(new ValidationResult.Builder().subject(PASSWORD)
                    .explanation(PASSWORD + " is required when using algorithm " + encryptionMethod.getAlgorithm()).build());
            return validationResults;
        }

        // If weak crypto is not explicitly allowed via override, check the password length and algorithm
        final int passwordBytesLength = password.getBytes(StandardCharsets.UTF_8).length;
        if (!allowWeakCrypto) {
            final int minimumSafePasswordLength = PasswordBasedEncryptor.getMinimumSafePasswordLength();
            if (passwordBytesLength < minimumSafePasswordLength) {
                validationResults.add(new ValidationResult.Builder().subject(PASSWORD)
                        .explanation("Password length less than " + minimumSafePasswordLength + " characters is potentially unsafe. See Admin Guide.").build());
            }
        }

        // Multiple checks on machine with limited strength crypto
        if (limitedStrengthCrypto) {
            // Cannot use unlimited strength ciphers on machine that lacks policies
            if (encryptionMethod.isUnlimitedStrength()) {
                validationResults.add(new ValidationResult.Builder().subject(ENCRYPTION_ALGORITHM)
                        .explanation(encryptionMethod.name() + " (" + encryptionMethod.getAlgorithm() + ") is not supported by this JVM due to lacking JCE Unlimited " +
                                "Strength Jurisdiction Policy files. See Admin Guide.").build());
            }

            // Check if the password exceeds the limit
            final boolean passwordLongerThanLimit = !CipherUtility.passwordLengthIsValidForAlgorithmOnLimitedStrengthCrypto(passwordBytesLength, encryptionMethod);
            if (passwordLongerThanLimit) {
                int maxPasswordLength = CipherUtility.getMaximumPasswordLengthForAlgorithmOnLimitedStrengthCrypto(encryptionMethod);
                validationResults.add(new ValidationResult.Builder().subject(PASSWORD)
                        .explanation("Password length greater than " + maxPasswordLength + " characters is not supported by this JVM" +
                                " due to lacking JCE Unlimited Strength Jurisdiction Policy files. See Admin Guide.").build());
            }
        }

        // Check the KDF for compatibility with this algorithm
        List<String> kdfsForPBECipher = getKDFsForPBECipher(encryptionMethod);
        if (kdf == null || !kdfsForPBECipher.contains(kdf.name())) {
            final String displayName = KEY_DERIVATION_FUNCTION;
            validationResults.add(new ValidationResult.Builder().subject(displayName)
                    .explanation(displayName + " is required to be " + StringUtils.join(kdfsForPBECipher,
                            ", ") + " when using algorithm " + encryptionMethod.getAlgorithm() + ". See Admin Guide.").build());
        }

        return validationResults;
    }

    public static List<ValidationResult> validateKeyed(EncryptionMethod encryptionMethod, KeyDerivationFunction kdf, String keyHex) {
        List<ValidationResult> validationResults = new ArrayList<>();
        boolean limitedStrengthCrypto = !PasswordBasedEncryptor.supportsUnlimitedStrength();

        if (limitedStrengthCrypto) {
            if (encryptionMethod.isUnlimitedStrength()) {
                validationResults.add(new ValidationResult.Builder().subject(ENCRYPTION_ALGORITHM)
                        .explanation(encryptionMethod.name() + " (" + encryptionMethod.getAlgorithm() + ") is not supported by this JVM due to lacking JCE Unlimited " +
                                "Strength Jurisdiction Policy files. See Admin Guide.").build());
            }
        }
        int allowedKeyLength = PasswordBasedEncryptor.getMaxAllowedKeyLength(ENCRYPTION_ALGORITHM);

        if (StringUtils.isEmpty(keyHex)) {
            validationResults.add(new ValidationResult.Builder().subject(RAW_KEY_HEX)
                    .explanation(RAW_KEY_HEX + " is required when using algorithm " + encryptionMethod.getAlgorithm() + ". See Admin Guide.").build());
        } else {
            byte[] keyBytes = new byte[0];
            try {
                keyBytes = Hex.decodeHex(keyHex.toCharArray());
            } catch (DecoderException e) {
                validationResults.add(new ValidationResult.Builder().subject(RAW_KEY_HEX)
                        .explanation("Key must be valid hexadecimal string. See Admin Guide.").build());
            }
            if (keyBytes.length * 8 > allowedKeyLength) {
                validationResults.add(new ValidationResult.Builder().subject(RAW_KEY_HEX)
                        .explanation("Key length greater than " + allowedKeyLength + " bits is not supported by this JVM" +
                                " due to lacking JCE Unlimited Strength Jurisdiction Policy files. See Admin Guide.").build());
            }
            if (!CipherUtility.isValidKeyLengthForAlgorithm(keyBytes.length * 8, encryptionMethod.getAlgorithm())) {
                List<Integer> validKeyLengths = CipherUtility.getValidKeyLengthsForAlgorithm(encryptionMethod.getAlgorithm());
                validationResults.add(new ValidationResult.Builder().subject(RAW_KEY_HEX)
                        .explanation("Key must be valid length [" + StringUtils.join(validKeyLengths, ", ") + "]. See Admin Guide.").build());
            }
        }

        // Perform some analysis on the selected encryption algorithm to ensure the JVM can support it and the associated key

        List<String> kdfsForKeyedCipher = getKDFsForKeyedCipher();
        if (kdf == null || !kdfsForKeyedCipher.contains(kdf.name())) {
            validationResults.add(new ValidationResult.Builder().subject(KEY_DERIVATION_FUNCTION)
                    .explanation(KEY_DERIVATION_FUNCTION + " is required to be " + StringUtils.join(kdfsForKeyedCipher, ", ") + " when using algorithm " +
                            encryptionMethod.getAlgorithm()).build());
        }

        return validationResults;
    }

    static List<String> getKDFsForKeyedCipher() {
        List<String> kdfsForKeyedCipher = new ArrayList<>();
        kdfsForKeyedCipher.add(KeyDerivationFunction.NONE.name());
        for (KeyDerivationFunction k : KeyDerivationFunction.values()) {
            if (k.isStrongKDF()) {
                kdfsForKeyedCipher.add(k.name());
            }
        }
        return kdfsForKeyedCipher;
    }

    static List<String> getKDFsForPBECipher(EncryptionMethod encryptionMethod) {
        List<String> kdfsForPBECipher = new ArrayList<>();
        for (KeyDerivationFunction k : KeyDerivationFunction.values()) {
            // Add all weak (legacy) KDFs except NONE
            if (!k.isStrongKDF() && !k.equals(KeyDerivationFunction.NONE)) {
                kdfsForPBECipher.add(k.name());
                // If this algorithm supports strong KDFs, add them as well
            } else if ((encryptionMethod.isCompatibleWithStrongKDFs() && k.isStrongKDF())) {
                kdfsForPBECipher.add(k.name());
            }
        }
        return kdfsForPBECipher;
    }

    public interface Encryptor {
        StreamCallback getEncryptionCallback() throws Exception;

        StreamCallback getDecryptionCallback() throws Exception;
    }
}
