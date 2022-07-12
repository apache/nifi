package org.apache.nifi.toolkit.encryptconfig.util;

import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory;
import org.apache.nifi.properties.scheme.ProtectionScheme;
import org.apache.nifi.properties.scheme.StandardProtectionScheme;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import org.apache.nifi.toolkit.encryptconfig.util.XmlEncryptor;
import org.testng.reporters.Files;

import java.io.File;
import java.io.IOException;
import java.util.List;

class XmlEncryptorTest {

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210";
    static final ProtectionScheme DEFAULT_PROTECTION_SCHEME = new StandardProtectionScheme("aes/gcm");

    @Test
    void TestDecrypt() throws IOException {
        XmlEncryptor encryptor = intializeXmlEncrytor();
        File inputAuthorizersFile = new File("src/test/resources/authorizers-populated.xml");
        String unencryptedFile = Files.readFile(inputAuthorizersFile);
        encryptor.encrypt(unencryptedFile);
    }

    private XmlEncryptor intializeXmlEncrytor() {
        final SensitivePropertyProvider sppEncrypt = StandardSensitivePropertyProviderFactory.withKey(KEY_HEX_128).getProvider(DEFAULT_PROTECTION_SCHEME);
        final SensitivePropertyProvider sppDecrypt = StandardSensitivePropertyProviderFactory.withKey(KEY_HEX_128).getProvider(DEFAULT_PROTECTION_SCHEME);
        final SensitivePropertyProviderFactory sppFactory = StandardSensitivePropertyProviderFactory.withKey(KEY_HEX_128);

        return new XmlEncryptor(sppEncrypt, sppDecrypt, sppFactory) {
            @Override
            public List<String> serializeXmlContentAndPreserveFormat(String updatedXmlContent, String originalXmlContent) {
                return null;
            }
        };
    }


}