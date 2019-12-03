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
package org.apache.nifi.properties.sensitive;

import org.apache.nifi.properties.StandardNiFiProperties;
import org.apache.nifi.util.NiFiProperties;
import org.bouncycastle.util.encoders.Base64;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public abstract class AbstractSensitivePropertyProviderTest {
    public void checkProviderCanProtectAndUnprotectValue(SensitivePropertyProvider sensitivePropertyProvider, int plainSize) {
        String plainText = CipherUtils.getRandomHex(plainSize);
        String cipherText = sensitivePropertyProvider.protect(plainText);

        Assert.assertNotNull(cipherText);
        Assert.assertNotEquals(plainText, cipherText);

        String unwrappedText = sensitivePropertyProvider.unprotect(cipherText);
        Assert.assertNotNull(unwrappedText);
        Assert.assertEquals(unwrappedText, plainText);
    }

    public void checkProviderProtectDoesNotAllowBlankValues(SensitivePropertyProvider sensitivePropertyProvider) throws Exception {
        final List<String> blankValues = new ArrayList<>(Arrays.asList("", "    ", "\n", "\n\n", "\t", "\t\t", "\t\n", "\n\t", null));
        for (String blank : blankValues) {
            boolean okay = false;
            try {
                sensitivePropertyProvider.protect(blank);
                okay = true;
            } catch (final IllegalArgumentException | SensitivePropertyProtectionException ignored) {
            }
            if (okay) {
                throw new Exception("SPP allowed empty string when it should not");
            }
        }
    }

    public void checkProviderUnprotectDoesNotAllowInvalidBase64Values(SensitivePropertyProvider sensitivePropertyProvider) throws Exception {
        final List<String> malformedCipherTextValues = new ArrayList<String>(Arrays.asList("any", "bad", "value"));

        // text that cannot be decoded values throw a bouncy castle exception:
        for (String malformedCipherTextValue : malformedCipherTextValues) {

            boolean okay = true;
            try {
                sensitivePropertyProvider.unprotect(malformedCipherTextValue);
            } catch (final IllegalArgumentException| SensitivePropertyProtectionException ignored) {
                okay = false;
            }
            if (okay) {
                throw new Exception("SPP allowed malformed ciphertext when it should not");
            }
        }
    }

    public void checkProviderUnprotectDoesNotAllowValidBase64InvalidCipherTextValues(SensitivePropertyProvider sensitivePropertyProvider) throws Exception {
        String plainText = CipherUtils.getRandomHex(128);
        String encodedText = Base64.toBase64String(plainText.getBytes());

        boolean okay = true;
        try {
            sensitivePropertyProvider.unprotect(encodedText);
        } catch (final SensitivePropertyProtectionException | IllegalArgumentException ignored) {
            okay = false;
        }
        if (okay) {
            throw new Exception("SPP allowed malformed ciphertext when it should not");
        }
    }

    public void checkProviderCanProtectAndUnprotectProperties(SensitivePropertyProvider sensitivePropertyProvider) throws Exception {
        final String propKey = NiFiProperties.SENSITIVE_PROPS_KEY;
        final String clearText = CipherUtils.getRandomHex(128);
        final Properties rawProps = new Properties();

        rawProps.setProperty(propKey, clearText); // set an unprotected value along with the specific key
        rawProps.setProperty(propKey + ".protected", sensitivePropertyProvider.getIdentifierKey());

        final NiFiProperties standardProps = new StandardNiFiProperties(rawProps);
        final ProtectedNiFiProperties protectedProps = new ProtectedNiFiProperties(standardProps, sensitivePropertyProvider.getIdentifierKey());

        // check to see if the property was encrypted
        final NiFiProperties encryptedProps = protectedProps.protectPlainProperties();
        Assert.assertNotEquals(clearText, encryptedProps.getProperty(propKey));
        Assert.assertEquals(clearText, sensitivePropertyProvider.unprotect(encryptedProps.getProperty(propKey)));
    }
}
