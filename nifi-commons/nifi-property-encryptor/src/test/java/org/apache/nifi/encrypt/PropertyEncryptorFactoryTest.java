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
package org.apache.nifi.encrypt;

import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class PropertyEncryptorFactoryTest {
    private static final EncryptionMethod ENCRYPTION_METHOD = EncryptionMethod.MD5_256AES;

    @Test
    public void testGetPropertyEncryptorUnsupportedEncryptionMethod() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM, EncryptionMethod.PGP.getAlgorithm());
        properties.setProperty(NiFiProperties.SENSITIVE_PROPS_PROVIDER, EncryptionMethod.PGP.getProvider());
        properties.setProperty(NiFiProperties.SENSITIVE_PROPS_KEY, String.class.getName());
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, properties);

        assertThrows(UnsupportedOperationException.class, () -> PropertyEncryptorFactory.getPropertyEncryptor(niFiProperties));
    }

    @Test
    public void testGetPropertyEncryptorPropertiesBlankPassword() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM, ENCRYPTION_METHOD.getAlgorithm());
        properties.setProperty(NiFiProperties.SENSITIVE_PROPS_PROVIDER, ENCRYPTION_METHOD.getProvider());
        properties.setProperty(NiFiProperties.SENSITIVE_PROPS_KEY, StringUtils.EMPTY);
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, properties);

        assertThrows(IllegalArgumentException.class, () -> PropertyEncryptorFactory.getPropertyEncryptor(niFiProperties));
    }

    @Test
    public void testGetPropertyEncryptorPropertiesKeyedCipherPropertyEncryptor() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.SENSITIVE_PROPS_ALGORITHM, PropertyEncryptionMethod.NIFI_ARGON2_AES_GCM_256.toString());
        properties.setProperty(NiFiProperties.SENSITIVE_PROPS_PROVIDER, ENCRYPTION_METHOD.getProvider());
        properties.setProperty(NiFiProperties.SENSITIVE_PROPS_KEY, String.class.getName());
        final NiFiProperties niFiProperties = NiFiProperties.createBasicNiFiProperties(null, properties);

        final PropertyEncryptor encryptor = PropertyEncryptorFactory.getPropertyEncryptor(niFiProperties);
        assertNotNull(encryptor);
        assertEquals(KeyedCipherPropertyEncryptor.class, encryptor.getClass());
    }
}
