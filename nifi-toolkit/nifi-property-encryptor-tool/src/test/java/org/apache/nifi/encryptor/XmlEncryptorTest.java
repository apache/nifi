package org.apache.nifi.encryptor;

import org.apache.nifi.util.crypto.CryptographyUtils;
import org.apache.nifi.util.file.FileUtilities;
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.properties.scheme.StandardProtectionScheme;
import org.apache.nifi.xml.XmlDecryptor;
import org.apache.nifi.xml.XmlEncryptor;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class XmlEncryptorTest {

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210";
    private static final String KEY_HEX_256 = KEY_HEX_128 + KEY_HEX_128;
    static final ProtectionScheme DEFAULT_PROTECTION_SCHEME = new StandardProtectionScheme("aes/gcm");
    public static final String KEY_HEX = CryptographyUtils.isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128;
    private static final String PASSWORD = "thisIsABadPassword";

    @Test
    void TestDecryptLoginIdentityProviders() throws URISyntaxException, IOException {
        String encryptedXmlFilename = "/login-identity-providers-populated-encrypted.xml";
        XmlDecryptor decryptor = intializeXmlDecryptor();
        File encryptedXmlFile = new File(XmlEncryptorTest.class.getResource(encryptedXmlFilename).toURI());
        File temporaryOutputFile = FileUtilities.getTemporaryOutputFile("decrypted", encryptedXmlFile);

        try (InputStream inputStream = new FileInputStream(encryptedXmlFile); FileOutputStream outputStream = new FileOutputStream(temporaryOutputFile)) {
             decryptor.decrypt(inputStream, outputStream);
        }

        assertEquals(3, verifyFileContains(temporaryOutputFile, PASSWORD));

        System.out.println("Decrypted to: " + temporaryOutputFile.getAbsolutePath());
    }

    @Test
    void TestEncryptLoginIdentityProviders() throws URISyntaxException, IOException {
        String unencryptedXmlFilename = "/login-identity-providers-populated-unecrypted.xml";
        XmlEncryptor encryptor = intializeXmlEncryptor();
        File unencryptedXmlFile = new File(XmlEncryptorTest.class.getResource(unencryptedXmlFilename).toURI());
        File temporaryOutputFile = FileUtilities.getTemporaryOutputFile("encrypted", unencryptedXmlFile);

        try (InputStream inputStream = new FileInputStream(unencryptedXmlFile);
             FileOutputStream outputStream = new FileOutputStream(temporaryOutputFile)) {
             encryptor.encrypt(inputStream, outputStream);
        }

        assertEquals(2, verifyFileContains(temporaryOutputFile, DEFAULT_PROTECTION_SCHEME.getPath()));

        System.out.println("Encrypted to: " + temporaryOutputFile.getAbsolutePath());
    }

    @Test
    void TestEncryptAuthorizers() throws URISyntaxException, IOException {
        String unencryptedXmlFilename = "/authorizers-populated-unencrypted.xml";
        XmlEncryptor encryptor = intializeXmlEncryptor();
        File unencryptedXmlFile = new File(XmlEncryptorTest.class.getResource(unencryptedXmlFilename).toURI());
        File temporaryOutputFile = FileUtilities.getTemporaryOutputFile("encrypted", unencryptedXmlFile);

        try (InputStream inputStream = new FileInputStream(unencryptedXmlFile);
             FileOutputStream outputStream = new FileOutputStream(temporaryOutputFile)) {
             encryptor.encrypt(inputStream, outputStream);
        }

        assertEquals(3, verifyFileContains(temporaryOutputFile, DEFAULT_PROTECTION_SCHEME.getPath()));

        System.out.println("Encrypted to: " + temporaryOutputFile.getAbsolutePath());
    }

    private XmlEncryptor intializeXmlEncryptor() {
        return new XmlEncryptor(StandardSensitivePropertyProviderFactory.withKey(KEY_HEX), DEFAULT_PROTECTION_SCHEME);
    }

    private XmlDecryptor intializeXmlDecryptor() {
        return new XmlDecryptor(StandardSensitivePropertyProviderFactory.withKey(KEY_HEX), DEFAULT_PROTECTION_SCHEME);
    }

    private int verifyFileContains(final File fileToCheck, final String fileContent) throws IOException {
        int count = 0;

        List<String> lines = Files.readAllLines(fileToCheck.toPath());
        for (String line : lines) {
            if (line.contains(fileContent)) {
                count++;
            }
        }
        return count;
    }
}