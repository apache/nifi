package org.apache.nifi.encryptor;

import org.apache.nifi.file.util.FileUtilities;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.properties.scheme.StandardProtectionScheme;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;

import static org.junit.jupiter.api.Assertions.*;

class XmlEncryptorTest {

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210";
    private static final String KEY_HEX_256 = KEY_HEX_128 + KEY_HEX_128;
    static final ProtectionScheme DEFAULT_PROTECTION_SCHEME = new StandardProtectionScheme("aes/gcm");
    //public static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128;
    private static final String PASSWORD = "thisIsABadPassword";
    private static final String ANOTHER_PASSWORD = "thisIsAnotherBadPassword";

    @Test
    void loadXmlFile() {

    }

    @Test
    void TestDecryptLoginIdentityProviders() throws URISyntaxException, IOException {
        String encryptedXmlFilename = "/login-identity-providers-populated-encrypted.xml";
        XmlEncryptor encryptor = intializeXmlEncrytor();
        File encryptedXmlFile = new File(XmlEncryptorTest.class.getResource(encryptedXmlFilename).toURI());
        File temporaryOutputFile = FileUtilities.getTemporaryOutputFile("decrypted", encryptedXmlFile);

        try (InputStream inputStream = new FileInputStream(encryptedXmlFile);
             FileOutputStream outputStream = new FileOutputStream(temporaryOutputFile)) {
             encryptor.decrypt(inputStream, outputStream);
        }

        System.out.println("Logged");
    }



//    private void writeLoginCredentials(final SingleUserCredentials singleUserCredentials, final InputStream inputStream) throws IOException, XMLStreamException {
//        try (final OutputStream outputStream = new FileOutputStream(providersFile)) {
//            final XMLEventWriter providersWriter = getProvidersWriter(outputStream);
//            final XMLEventReader providersReader = getProvidersReader(inputStream);
//            updateLoginIdentityProviders(singleUserCredentials, providersReader, providersWriter);
//            providersReader.close();
//            providersWriter.close();
//        }
//    }

    @Test
    void getXMLReader() {
    }

    private XmlEncryptor intializeXmlEncrytor() {
        // Sanity check for decryption
        String propertyName = "Manager Password";
        String cipherText = "q4r7WIgN0MaxdAKM||SGgdCTPGSFEcuH4RraMYEdeyVbOx93abdWTVSWvh1w+klA";
        String EXPECTED_PASSWORD = "thisIsABadPassword";
        final SensitivePropertyProvider sppEncrypt = StandardSensitivePropertyProviderFactory.withKey(KEY_HEX_128).getProvider(DEFAULT_PROTECTION_SCHEME);
        final SensitivePropertyProvider sppDecrypt = StandardSensitivePropertyProviderFactory.withKey(KEY_HEX_128).getProvider(DEFAULT_PROTECTION_SCHEME);
        final SensitivePropertyProviderFactory sppFactory = StandardSensitivePropertyProviderFactory.withKey(KEY_HEX_128);

        return new XmlEncryptor(sppEncrypt, sppDecrypt, sppFactory);
//        assert spp.unprotect(cipherText, ldapPropertyContext(propertyName)) == EXPECTED_PASSWORD
    }

}