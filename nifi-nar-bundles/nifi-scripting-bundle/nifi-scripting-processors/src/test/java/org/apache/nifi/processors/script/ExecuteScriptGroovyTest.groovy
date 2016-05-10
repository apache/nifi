package org.apache.nifi.processors.script

import org.apache.nifi.security.util.EncryptionMethod
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.python.bouncycastle.util.encoders.Hex
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher

@RunWith(JUnit4.class)
class ExecuteScriptGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ExecuteScriptGroovyTest.class)

    @BeforeClass
    public static void setUpOnce() throws Exception {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testShouldExecuteScript() throws Exception {
        // Arrange
        final String PASSWORD = "shortPassword";
        final byte[] SALT = cipherProvider.generateSalt()

        final String plaintext = "This is a plaintext message.";

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            logger.info("Using algorithm: ${em.getAlgorithm()}");

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, PASSWORD, SALT, DEFAULT_KEY_LENGTH, true);
            byte[] iv = cipher.getIV();
            logger.info("IV: ${Hex.encodeHexString(iv)}")

            byte[] cipherBytes = cipher.doFinal(plaintext.getBytes("UTF-8"));
            logger.info("Cipher text: ${Hex.encodeHexString(cipherBytes)} ${cipherBytes.length}");

            cipher = cipherProvider.getCipher(em, PASSWORD, SALT, iv, DEFAULT_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");
            logger.info("Recovered: ${recovered}")

            // Assert
            assert plaintext.equals(recovered);
        }
    }

    //testShouldExecuteScriptWithPool
    //testShouldHandleFailingScript
    //testShouldHandleNoAvailableEngine
    //testPooledExecutionShouldBeFaster
}
