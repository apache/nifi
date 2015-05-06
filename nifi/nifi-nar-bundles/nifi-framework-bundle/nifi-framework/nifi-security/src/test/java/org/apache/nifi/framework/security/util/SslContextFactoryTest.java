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
package org.apache.nifi.framework.security.util;

import org.apache.nifi.security.util.KeystoreType;
import java.io.File;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 */
public class SslContextFactoryTest {

    private NiFiProperties mutualAuthProps;
    private NiFiProperties authProps;

    @Before
    public void setUp() throws Exception {

        final File ksFile = new File(SslContextFactoryTest.class.getResource("/localhost-ks.jks").toURI());
        final File trustFile = new File(SslContextFactoryTest.class.getResource("/localhost-ts.jks").toURI());

        authProps = mock(NiFiProperties.class);
        when(authProps.getProperty(NiFiProperties.SECURITY_KEYSTORE)).thenReturn(ksFile.getAbsolutePath());
        when(authProps.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE)).thenReturn(KeystoreType.JKS.toString());
        when(authProps.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD)).thenReturn("localtest");
        when(authProps.getNeedClientAuth()).thenReturn(false);

        mutualAuthProps = mock(NiFiProperties.class);
        when(mutualAuthProps.getProperty(NiFiProperties.SECURITY_KEYSTORE)).thenReturn(ksFile.getAbsolutePath());
        when(mutualAuthProps.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE)).thenReturn(KeystoreType.JKS.toString());
        when(mutualAuthProps.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD)).thenReturn("localtest");
        when(mutualAuthProps.getProperty(NiFiProperties.SECURITY_TRUSTSTORE)).thenReturn(trustFile.getAbsolutePath());
        when(mutualAuthProps.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE)).thenReturn(KeystoreType.JKS.toString());
        when(mutualAuthProps.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD)).thenReturn("localtest");
        when(mutualAuthProps.getNeedClientAuth()).thenReturn(true);

    }

    @Test
    public void testCreateSslContextWithMutualAuth() {
        Assert.assertNotNull(SslContextFactory.createSslContext(mutualAuthProps));
    }

    @Test
    public void testCreateSslContextWithNoMutualAuth() {
        Assert.assertNotNull(SslContextFactory.createSslContext(authProps));
    }

}
