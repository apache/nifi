package org.apache.nifi.processors.standard.util;

import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.security.util.EncryptionMethod;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class NiFiLegacyKeyDeriverTest {
    private static final Logger logger = LoggerFactory.getLogger(NiFiLegacyKeyDeriverTest.class);

    private static List<EncryptionMethod> pbeEncryptionMethods = new ArrayList<>();
    private static List<EncryptionMethod> limitedStrengthPbeEncryptionMethods = new ArrayList<>();

    private static final String PROVIDER_NAME = "BC";
    private static final int ITERATION_COUNT = 1000;

    @BeforeClass
    public static void setUpOnce() throws Exception {
        for (EncryptionMethod em : EncryptionMethod.values()) {
            if (em.getAlgorithm().toUpperCase().startsWith("PBE")) {
                pbeEncryptionMethods.add(em);
                if (!em.isUnlimitedStrength()) {
                    limitedStrengthPbeEncryptionMethods.add(em);
                }
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        Security.addProvider(new BouncyCastleProvider());
    }

    @After
    public void tearDown() throws Exception {

    }

    private static Cipher getLegacyCipher(String password, byte[] salt, String algorithm) {
        try {
            final PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray());
            final SecretKeyFactory factory = SecretKeyFactory.getInstance(algorithm, PROVIDER_NAME);
            SecretKey tempKey = factory.generateSecret(pbeKeySpec);

            final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, ITERATION_COUNT);
            Cipher cipher = Cipher.getInstance(algorithm, PROVIDER_NAME);
            cipher.init(Cipher.DECRYPT_MODE, tempKey, parameterSpec);
            return cipher;
        } catch (Exception e) {
            logger.error("Error generating legacy cipher", e);
            fail(e.getMessage());
        }

        return null;
    }

    @Test
    public void testDeriveKey() throws Exception {

    }

    @Test
    public void testGenerateSaltShouldMatchLegacyLengths() throws Exception {
        // Arrange
        int legacySaltSize;
        Cipher cipher;

        NiFiLegacyKeyDeriver keyDeriver;

        // Act
        for (EncryptionMethod em : pbeEncryptionMethods) {
            cipher = Cipher.getInstance(em.getAlgorithm(), em.getProvider());
            legacySaltSize = cipher.getBlockSize();

            keyDeriver = new NiFiLegacyKeyDeriver(em.getAlgorithm());

            // Assert
            assert keyDeriver.generateSalt().length == legacySaltSize;
        }
    }

    @Test
    public void testDeriveKeyShouldMatchLegacyOutput() throws Exception {
        // Arrange
        NiFiLegacyKeyDeriver keyDeriver;

        final String PASSWORD = "shortPassword";
        final byte[] SALT = Hex.decodeHex("aabbccddeeff0011".toCharArray());

        final String plaintext = "This is a plaintext message.";

        // Act
        for (EncryptionMethod em : limitedStrengthPbeEncryptionMethods) {
            // Derive a key and IV from the password and salt using the legacy code
            final Cipher legacyCipher = getLegacyCipher(PASSWORD, SALT, em.getAlgorithm());
            logger.info("Expected  IV: {}", Hex.encodeHexString(legacyCipher.getIV()));

            keyDeriver = new NiFiLegacyKeyDeriver(em.getAlgorithm());

            SecretKey newKey = keyDeriver.deriveKey(PASSWORD, SALT);
            logger.info("Derived  key: {}", Hex.encodeHexString(newKey.getEncoded()));

            byte[] newIV = keyDeriver.getIV(PASSWORD, SALT);
            logger.info("Derived   IV: {}", Hex.encodeHexString(newIV));

            // Initialize a cipher with the manually-derived parameters
            Cipher newCipher = Cipher.getInstance(em.getAlgorithm(), PROVIDER_NAME);
            newCipher.init(Cipher.ENCRYPT_MODE, newKey, new IvParameterSpec(newIV));

            byte[] cipherBytes = newCipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.info("Cipher text: {} {}", Hex.encodeHexString(cipherBytes), cipherBytes.length);
            byte[] recoveredBytes = legacyCipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assert plaintext.equals(recovered);
            assert newIV == legacyCipher.getIV();
        }
    }

    @Test
    public void testDeriveKeyShouldBeInternallyConsistent() throws Exception {
        // Arrange
        NiFiLegacyKeyDeriver keyDeriver;

        final String PASSWORD = "shortPassword";
        final byte[] SALT = Hex.decodeHex("aabbccddeeff0011".toCharArray());

        final String plaintext = "This is a plaintext message.";

        // Act
        for (EncryptionMethod em : limitedStrengthPbeEncryptionMethods) {
            keyDeriver = new NiFiLegacyKeyDeriver(em.getAlgorithm());

            SecretKey newKey = keyDeriver.deriveKey(PASSWORD, SALT);
            logger.info("Derived  key: {}", Hex.encodeHexString(newKey.getEncoded()));

            byte[] newIV = keyDeriver.getIV(PASSWORD, SALT);
            logger.info("Derived   IV: {}", Hex.encodeHexString(newIV));

            // Initialize a cipher with the manually-derived parameters
            Cipher cipher = Cipher.getInstance(em.getAlgorithm(), PROVIDER_NAME);
            final IvParameterSpec ivParameterSpec = new IvParameterSpec(newIV);
            cipher.init(Cipher.ENCRYPT_MODE, newKey, ivParameterSpec);

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.info("Cipher text: {} {}", Hex.encodeHexString(cipherBytes), cipherBytes.length);

            cipher.init(Cipher.DECRYPT_MODE, newKey, ivParameterSpec);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assert plaintext.equals(recovered);
        }
    }

    @Ignore("Diagnostic test only")
    @Test
    public void testShouldDetermineAllCipherSaltSizes() throws Exception {
        // Arrange
        int saltSize;
        Cipher cipher;
        final int EXPECTED_SALT_SIZE = 16;
        boolean nonStandardSaltSize = false;

        // Act
        for (EncryptionMethod em : pbeEncryptionMethods) {
            cipher = Cipher.getInstance(em.getAlgorithm(), em.getProvider());
            saltSize = cipher.getBlockSize();
            logger.info("{}\t{}\t{}", em.name(), em.getAlgorithm(), saltSize);
            if (saltSize != EXPECTED_SALT_SIZE) {
                nonStandardSaltSize = true;
            }
        }

        // Assert
        assert !nonStandardSaltSize;
    }
}