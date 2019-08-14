package org.apache.nifi.stateless.core;

import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.util.NiFiProperties;

public class NiFiPropertiesUtil {

    public static StringEncryptor createEncryptorFromProperties(NiFiProperties nifiProperties) {
        final String algorithm = nifiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM);
        final String provider = nifiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_PROVIDER);
        final String password = nifiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY);
        return StringEncryptor.createEncryptor(algorithm, provider, password);
    }
}
