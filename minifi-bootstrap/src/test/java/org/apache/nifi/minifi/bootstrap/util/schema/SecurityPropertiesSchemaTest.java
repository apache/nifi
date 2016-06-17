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

package org.apache.nifi.minifi.bootstrap.util.schema;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SecurityPropertiesSchemaTest {
    private SecurityPropertiesSchema securityPropertiesSchema;

    @Before
    public void setup() {
        securityPropertiesSchema = new SecurityPropertiesSchema(new HashMap());
    }

    @Test
    public void testKeystoreDefault() {
        assertEquals("", securityPropertiesSchema.getKeystore());
    }

    @Test
    public void testTruststoreDefault() {
        assertEquals("", securityPropertiesSchema.getTruststore());
    }

    @Test
    public void testSslProtocolDefault() {
        assertEquals("", securityPropertiesSchema.getSslProtocol());
    }

    @Test
    public void testKeystoreTypeDefault() {
        assertEquals("", securityPropertiesSchema.getKeystoreType());
    }

    @Test
    public void testKeyStorePasswdDefault() {
        assertEquals("", securityPropertiesSchema.getKeystorePassword());
    }

    @Test
    public void testKeyPasswordDefault() {
        assertEquals("", securityPropertiesSchema.getKeyPassword());
    }

    @Test
    public void testTruststoreTypeDefault() {
        assertEquals("", securityPropertiesSchema.getTruststoreType());
    }

    @Test
    public void testTruststorePasswdDefault() {
        assertEquals("", securityPropertiesSchema.getTruststorePassword());
    }

    @Test
    public void testEmptyMapConstructorValid() {
        assertTrue(securityPropertiesSchema.isValid());
    }
}
