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
package org.apache.nifi.properties.scheme;

import org.apache.nifi.properties.SensitivePropertyProtectionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandardProtectionSchemeResolverTest {
    private static final String AES_GCM = "AES_GCM";

    private static final String AES_GCM_PATH = "aes/gcm";

    private static final String AES_GCM_256_PATH = "aes/gcm/256";

    private static final String UNKNOWN = "UNKNOWN";

    private StandardProtectionSchemeResolver resolver;

    @BeforeEach
    public void setResolver() {
        resolver = new StandardProtectionSchemeResolver();
    }

    @Test
    public void getProtectionSchemeAesGcmFound() {
        final ProtectionScheme protectionScheme = resolver.getProtectionScheme(AES_GCM);
        assertNotNull(protectionScheme);
        assertEquals(AES_GCM_PATH, protectionScheme.getPath());
    }

    @Test
    public void getProtectionSchemeAesGcm256Found() {
        final ProtectionScheme protectionScheme = resolver.getProtectionScheme(AES_GCM_256_PATH);
        assertNotNull(protectionScheme);
        assertEquals(AES_GCM_PATH, protectionScheme.getPath());
    }

    @Test
    public void getProtectionSchemeUnknownNotFound() {
        final SensitivePropertyProtectionException exception = assertThrows(SensitivePropertyProtectionException.class, () -> resolver.getProtectionScheme(UNKNOWN));
        assertTrue(exception.getMessage().contains(UNKNOWN));
    }
}
