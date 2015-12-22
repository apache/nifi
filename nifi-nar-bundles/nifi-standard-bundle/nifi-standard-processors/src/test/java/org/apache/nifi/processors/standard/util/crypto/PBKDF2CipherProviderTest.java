package org.apache.nifi.processors.standard.util.crypto;

import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.security.util.EncryptionMethod;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

@RunWith(JUnit4.class)
public class PBKDF2CipherProviderTest {
    private static final Logger logger = LoggerFactory.getLogger(PBKDF2CipherProviderTest.class);

    private static List<EncryptionMethod> pbeEncryptionMethods = new ArrayList<>();
    private static List<EncryptionMethod> limitedStrengthPbeEncryptionMethods = new ArrayList<>();

    private static final String PROVIDER_NAME = "BC";
    private static final int ITERATION_COUNT = 0;

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
            cipher.init(Cipher.ENCRYPT_MODE, tempKey, parameterSpec);
            return cipher;
        } catch (Exception e) {
            logger.error("Error generating legacy cipher", e);
            fail(e.getMessage());
        }

        return null;
    }

    @Test
    public void testGetCipherShouldBeInternallyConsistent() throws Exception {
        // Arrange
        OpenSSLPKCS5CipherProvider cipherProvider = new OpenSSLPKCS5CipherProvider();

        final String PASSWORD = "shortPassword";
        final byte[] SALT = Hex.decodeHex("aabbccddeeff0011".toCharArray());

        final String plaintext = "This is a plaintext message.";

        // Act
        for (EncryptionMethod em : limitedStrengthPbeEncryptionMethods) {
            logger.debug("Using algorithm: {}", em.getAlgorithm());

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em.getAlgorithm(), em.getProvider(), PASSWORD, SALT, true);

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.debug("Cipher text: {} {}", Hex.encodeHexString(cipherBytes), cipherBytes.length);

            cipher = cipherProvider.getCipher(em.getAlgorithm(), em.getProvider(), PASSWORD, SALT, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assert plaintext.equals(recovered);
        }
    }

    @Test
    public void testGetCipherWithUnlimitedStrengthShouldBeInternallyConsistent() throws Exception {
        // Arrange
        Assume.assumeTrue("Test is being skipped due to this JVM lacking JCE Unlimited Strength Jurisdiction Policy file.",
                PasswordBasedEncryptor.supportsUnlimitedStrength());

        OpenSSLPKCS5CipherProvider cipherProvider = new OpenSSLPKCS5CipherProvider();

        final String PASSWORD = "shortPassword";
        final byte[] SALT = Hex.decodeHex("aabbccddeeff0011".toCharArray());

        final String plaintext = "This is a plaintext message.";

        // Act
        for (EncryptionMethod em : pbeEncryptionMethods) {
            logger.debug("Using algorithm: {}", em.getAlgorithm());

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em.getAlgorithm(), em.getProvider(), PASSWORD, SALT, true);

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.debug("Cipher text: {} {}", Hex.encodeHexString(cipherBytes), cipherBytes.length);

            cipher = cipherProvider.getCipher(em.getAlgorithm(), em.getProvider(), PASSWORD, SALT, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assert plaintext.equals(recovered);
        }
    }

    // TODO: testGetKeySizeShouldHandleAllPBEAlgorithms
    // TODO: testShouldResolvePRF
    // TODO: testGetCipherShouldTruncateKeyToCorrectSize
    // TODO: testArgumentInput

    @Test
    public void testGetCipherShouldSupportLegacyCode() throws Exception {
        // Arrange
        OpenSSLPKCS5CipherProvider cipherProvider = new OpenSSLPKCS5CipherProvider();

        final String PASSWORD = "shortPassword";
        final byte[] SALT = Hex.decodeHex("0011223344556677".toCharArray());

        final String plaintext = "This is a plaintext message.";

        // Act
        for (EncryptionMethod em : limitedStrengthPbeEncryptionMethods) {
            logger.debug("Using algorithm: {}", em.getAlgorithm());

            // Initialize a legacy cipher for encryption
            Cipher legacyCipher = getLegacyCipher(PASSWORD, SALT, em.getAlgorithm());

            byte[] cipherBytes = legacyCipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.debug("Cipher text: {} {}", Hex.encodeHexString(cipherBytes), cipherBytes.length);

            Cipher providedCipher = cipherProvider.getCipher(em.getAlgorithm(), em.getProvider(), PASSWORD, SALT, false);
            byte[] recoveredBytes = providedCipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assert plaintext.equals(recovered);
        }
    }

    @Test
    public void testGetCipherWithoutSaltShouldSupportLegacyCode() throws Exception {
        // Arrange
        OpenSSLPKCS5CipherProvider cipherProvider = new OpenSSLPKCS5CipherProvider();

        final String PASSWORD = "shortPassword";
        final byte[] SALT = new byte[0];

        final String plaintext = "This is a plaintext message.";

        // Act
        for (EncryptionMethod em : limitedStrengthPbeEncryptionMethods) {
            logger.debug("Using algorithm: {}", em.getAlgorithm());

            // Initialize a legacy cipher for encryption
            Cipher legacyCipher = getLegacyCipher(PASSWORD, SALT, em.getAlgorithm());

            byte[] cipherBytes = legacyCipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.debug("Cipher text: {} {}", Hex.encodeHexString(cipherBytes), cipherBytes.length);

            Cipher providedCipher = cipherProvider.getCipher(em.getAlgorithm(), em.getProvider(), PASSWORD, false);
            byte[] recoveredBytes = providedCipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assert plaintext.equals(recovered);
        }
    }
}