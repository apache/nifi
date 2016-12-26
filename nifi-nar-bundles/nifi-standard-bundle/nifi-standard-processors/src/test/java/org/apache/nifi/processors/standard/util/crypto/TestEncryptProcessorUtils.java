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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processors.standard.EncryptContent;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class TestEncryptProcessorUtils {

    private static final Logger logger = LoggerFactory.getLogger(TestEncryptProcessorUtils.class);

    @Test
    public void testShouldDetermineMaxKeySizeForAlgorithms() throws IOException {
        // Arrange
        final String AES_ALGORITHM = EncryptionMethod.MD5_256AES.getAlgorithm();
        final String DES_ALGORITHM = EncryptionMethod.MD5_DES.getAlgorithm();

        final int AES_MAX_LENGTH = PasswordBasedEncryptor.supportsUnlimitedStrength() ? Integer.MAX_VALUE : 128;
        final int DES_MAX_LENGTH = PasswordBasedEncryptor.supportsUnlimitedStrength() ? Integer.MAX_VALUE : 64;

        // Act
        int determinedAESMaxLength = PasswordBasedEncryptor.getMaxAllowedKeyLength(AES_ALGORITHM);
        int determinedTDESMaxLength = PasswordBasedEncryptor.getMaxAllowedKeyLength(DES_ALGORITHM);

        // Assert
        assert determinedAESMaxLength == AES_MAX_LENGTH;
        assert determinedTDESMaxLength == DES_MAX_LENGTH;
    }

    @Test
    public void testValidPasswordForPBE() {

        EncryptionMethod em = EncryptionMethod.MD5_128AES;
        KeyDerivationFunction kdf = KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY;
        String password = "helloworld";
        String shortPassword = "short";

        logger.info("Testing PBE with password '{}'", password);
        List<ValidationResult> vr = EncryptProcessorUtils.validatePBE(em, kdf, password, true);
        Assert.assertEquals(0, vr.size());


        logger.info("Testing PBE with password '{}' and weak crypto disabled", shortPassword);

        final int minimumSafePasswordLength = PasswordBasedEncryptor.getMinimumSafePasswordLength();
        List<ValidationResult> results = EncryptProcessorUtils.validatePBE(em, kdf, shortPassword, false);
        String expectedResult = "Password length less than " + minimumSafePasswordLength;
        String message = "'" + results.get(0).toString() + "' should contain (" + expectedResult + ")";
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(message, results.get(0).toString().contains(expectedResult));

        logger.info("Test successfully completed");
    }

    @Test
    public void testEmptyPasswordForPBE() {
        EncryptionMethod em = EncryptionMethod.MD5_128AES;
        KeyDerivationFunction kdf = KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY;

        logger.info("Testing with password '{}'", "");

        List<ValidationResult> results = EncryptProcessorUtils.validatePBE(em, kdf, "", true);
        String expectedResult = EncryptProcessorUtils.PASSWORD + " is required";
        String message = "'" + results.get(0).toString() + "' should contain (" + expectedResult + ")";
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(message, results.get(0).toString().contains(expectedResult));

        logger.info("Test successfully completed");
    }

    @Test
    public void testValidKdfForPBE() {
        EncryptionMethod em = EncryptionMethod.MD5_128AES;
        KeyDerivationFunction kdf = KeyDerivationFunction.BCRYPT;
        String password = "password";

        logger.info("Testing PBE with KDF Bcrypt");

        List<ValidationResult> results = EncryptProcessorUtils.validatePBE(em, kdf, password, true);
        List<String> kdfsForPBECipher = EncryptProcessorUtils.getKDFsForPBECipher(em);
        ValidationResult expectedResult = new ValidationResult.Builder().subject(EncryptProcessorUtils.KEY_DERIVATION_FUNCTION)
                .explanation(EncryptProcessorUtils.KEY_DERIVATION_FUNCTION + " is required to be " +
                        StringUtils.join(kdfsForPBECipher, ", ") + " when using algorithm " + em.getAlgorithm() +
                        ". See Admin Guide.").build();
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(results.contains(expectedResult));
        logger.info("Test complete");
    }

    @Test
    public void testValidHexKeysForKeyedCipher() {
        EncryptionMethod em = EncryptionMethod.AES_CBC;
        KeyDerivationFunction kdf = KeyDerivationFunction.BCRYPT;
        List<ValidationResult> results;
        String hexKey = "abababababababababababababababab";


        logger.info("Test started for Keyed Cipher with {}", em.name());
        logger.info("Testing with hex key '{}'", hexKey);
        results = EncryptProcessorUtils.validateKeyed(em, kdf, hexKey);
        Assert.assertEquals(0, results.size());

        logger.info("Testing with hex key '{}'", "abab");
        results = EncryptProcessorUtils.validateKeyed(em, kdf, "abab");
        String expectedResult = "Key must be valid length";
        String message = "'" + results.get(0).toString() + "' should contain (" + expectedResult + ")";
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(message, results.get(0).toString().contains(expectedResult));

        logger.info("Test successfully completed");
    }

    @Test
    public void testEmptyHexKeyForKeyedCipher() {
        EncryptionMethod em = EncryptionMethod.AES_CBC;
        KeyDerivationFunction kdf = KeyDerivationFunction.BCRYPT;

        logger.info("Testing with {} and empty hex key", em.name());
        List<ValidationResult> results = EncryptProcessorUtils.validateKeyed(em, kdf, "");
        String expectedResult = EncryptProcessorUtils.RAW_KEY_HEX + " is required";
        String message = "'" + results.get(0).toString() + "' should contain (" + expectedResult + ")";
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(message, results.get(0).toString().contains(expectedResult));

        logger.info("Test successfully complete");
    }

    @Test
    public void testValidKdfForKeyedCipher() {
        String hexKey = "abababababababababababababababab";
        EncryptionMethod em = EncryptionMethod.AES_CBC;
        KeyDerivationFunction kdf = KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY;

        logger.info("Testing Keyed Cipher with {}", kdf.name());
        List<ValidationResult> results = EncryptProcessorUtils.validateKeyed(em, kdf, hexKey);
        String expectedResult = EncryptProcessorUtils.KEY_DERIVATION_FUNCTION + " is required to be ";
        String message = "'" + results.get(0).toString() + "' should contain (" + expectedResult + ")";
        Assert.assertEquals(1, results.size());
        Assert.assertTrue(message, results.get(0).toString().contains(expectedResult));
        logger.info("Test successfully completed");
    }


    @Test
    public void testShouldValidatePGPPublicKeyringExists() {
        logger.info("Testing PGP");

        EncryptionMethod em = EncryptionMethod.PGP;
        String publicKeyring = "src/test/resources/TestEncryptContent/pubring.sgpg.missing";
        String publicUserId = "USERID";

        logger.info("Public-user-id: {}", publicUserId);
        logger.info("Public-Keyring: {}", publicKeyring);

        List<ValidationResult> results = EncryptProcessorUtils.validatePGP(em, null, true, publicKeyring, publicUserId, null, null);

        // Assert
        Assert.assertEquals(1, results.size());
        ValidationResult vr = (ValidationResult) results.toArray()[0];
        String expectedResult = "java.io.FileNotFoundException";
        String message = "'" + vr.toString() + "' should contain '" + expectedResult + "'";
        Assert.assertTrue(message, vr.toString().contains(expectedResult));

        logger.info("Test successfully completed");
    }

    @Test
    public void testShouldValidatePGPPublicKeyringRequiresUserId() {
        logger.info("Testing PGP");

        EncryptionMethod em = EncryptionMethod.PGP;
        String publicKeyring = "src/test/resources/TestEncryptContent/pubring.gpg";
        String publicUserId = null;

        logger.info("Public-user-id: {}", publicUserId);
        logger.info("Public-Keyring: {}", publicKeyring);


        List<ValidationResult> results = EncryptProcessorUtils.validatePGP(em, null, true, publicKeyring, null, null, null);

        // Assert
        Assert.assertEquals(1, results.size());
        ValidationResult vr = (ValidationResult) results.toArray()[0];
        String expectedResult = " encryption without a " + EncryptContent.PASSWORD.getDisplayName() + " requires both "
                + EncryptContent.PUBLIC_KEYRING.getDisplayName() + " and "
                + EncryptContent.PUBLIC_KEY_USERID.getDisplayName();
        String message = "'" + vr.toString() + "' should contain '" + expectedResult + "'";
        Assert.assertTrue(message, vr.toString().contains(expectedResult));

        logger.info("Test successfully completed");
    }

    @Test
    public void testShouldValidatePGPPublicKeyringIsProperFormat() {
        logger.info("Testing PGP");

        EncryptionMethod em = EncryptionMethod.PGP;
        String publicKeyring = "src/test/resources/TestEncryptContent/text.txt";
        String publicUserId = "USERID";

        logger.info("Public-user-id: {}", publicUserId);
        logger.info("Public-Keyring: {}", publicKeyring);

        List<ValidationResult> results = EncryptProcessorUtils.validatePGP(em, null, true, publicKeyring, publicUserId, null, null);

        // Assert
        Assert.assertEquals(1, results.size());
        ValidationResult vr = (ValidationResult) results.toArray()[0];
        String expectedResult = " java.io.IOException: invalid header encountered";
        String message = "'" + vr.toString() + "' should contains '" + expectedResult + "'";
        Assert.assertTrue(message, vr.toString().contains(expectedResult));

        logger.info("Test successfully completed");
    }

    @Test
    public void testShouldValidatePGPPublicKeyringContainsUserId() {
        logger.info("Testing PGP");

        EncryptionMethod em = EncryptionMethod.PGP;
        String publicKeyring = "src/test/resources/TestEncryptContent/pubring.gpg";
        String publicUserId = "USERID";

        logger.info("Public-user-id: {}", publicUserId);
        logger.info("Public-Keyring: {}", publicKeyring);

        // Act
        List<ValidationResult> results = EncryptProcessorUtils.validatePGP(em,null,true,publicKeyring,publicUserId,null,null);

        // Assert
        Assert.assertEquals(1, results.size());
        ValidationResult vr = (ValidationResult) results.toArray()[0];
        String expectedResult = "PGPException: Could not find a public key with the given userId";
        String message = "'" + vr.toString() + "' should contain '" + expectedResult + "'";
        Assert.assertTrue(message, vr.toString().contains(expectedResult));

        logger.info("Test successfully completed");
    }

    @Test
    public void testShouldExtractPGPPublicKeyFromKeyring() {
        logger.info("Testing PGP");

        EncryptionMethod em = EncryptionMethod.PGP;
        String publicKeyring = "src/test/resources/TestEncryptContent/pubring.gpg";
        String publicUserId = "NiFi PGP Test Key (Short test key for NiFi PGP unit tests) <alopresto.apache+test@gmail.com>";

        logger.info("Public-user-id: {}", publicUserId);
        logger.info("Public-Keyring: {}", publicKeyring);

        // Act
        List<ValidationResult> results = EncryptProcessorUtils.validatePGP(em,null,true,publicKeyring,publicUserId,null,null);

        // Assert
        Assert.assertEquals(0, results.size());

        logger.info("Test successfully completed");
    }
}
