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

package org.apache.nifi.toolkit.tls.manager.writer;

import org.apache.nifi.toolkit.tls.configuration.TlsClientConfig;
import org.apache.nifi.toolkit.tls.configuration.TlsConfig;
import org.apache.nifi.toolkit.tls.properties.NiFiPropertiesWriter;
import org.apache.nifi.toolkit.tls.properties.NiFiPropertiesWriterFactory;
import org.apache.nifi.toolkit.tls.util.OutputStreamFactory;
import org.apache.nifi.util.NiFiProperties;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class NifiPropertiesTlsClientConfigWriterTest {
    @Mock
    NiFiPropertiesWriterFactory niFiPropertiesWriterFactory;
    @Mock
    OutputStreamFactory outputStreamFactory;
    private NiFiPropertiesWriter niFiPropertiesWriter;
    private int hostNum;
    private String testHostname;
    private File outputFile;
    private NifiPropertiesTlsClientConfigWriter nifiPropertiesTlsClientConfigWriter;
    private TlsClientConfig tlsClientConfig;
    private ByteArrayOutputStream outputStream;
    private String keyStore;
    private String keyStorePassword;
    private String trustStore;
    private String trustStorePassword;
    private Properties overlayProperties;
    private String keyPassword;
    private String keyStoreType;
    private String trustStoreType;

    @Before
    public void setup() throws IOException {
        testHostname = "testHostname";
        hostNum = 22;

        keyStore = "testKeyStore.jks";
        keyStoreType = TlsConfig.DEFAULT_KEY_STORE_TYPE;
        keyStorePassword = "badKeyStorePassword";
        keyPassword = "badKeyPassword";

        trustStore = "testTrustStore.jks";
        trustStoreType = TlsConfig.DEFAULT_KEY_STORE_TYPE;
        trustStorePassword = "badTrustStorePassword";

        outputFile = File.createTempFile("temp", "nifi");
        outputStream = new ByteArrayOutputStream();
        when(outputStreamFactory.create(outputFile)).thenReturn(outputStream);

        tlsClientConfig = new TlsClientConfig();
        tlsClientConfig.setKeyStore(keyStore);
        tlsClientConfig.setKeyStoreType(keyStoreType);
        tlsClientConfig.setKeyStorePassword(keyStorePassword);
        tlsClientConfig.setKeyPassword(keyPassword);

        tlsClientConfig.setTrustStore(trustStore);
        tlsClientConfig.setTrustStoreType(trustStoreType);
        tlsClientConfig.setTrustStorePassword(trustStorePassword);

        niFiPropertiesWriter = new NiFiPropertiesWriter(new ArrayList<>());
        when(niFiPropertiesWriterFactory.create()).thenReturn(niFiPropertiesWriter);
        nifiPropertiesTlsClientConfigWriter = new NifiPropertiesTlsClientConfigWriter(niFiPropertiesWriterFactory, outputFile, testHostname, hostNum);
        overlayProperties = nifiPropertiesTlsClientConfigWriter.getOverlayProperties();
    }

    @Test
    public void testDefaults() throws IOException {
        nifiPropertiesTlsClientConfigWriter.write(tlsClientConfig, outputStreamFactory);
        testHostnamesAndPorts();
        assertNotEquals(0, nifiPropertiesTlsClientConfigWriter.getIncrementingPropertyMap().size());
    }

    @Test(expected = NumberFormatException.class)
    public void testBadPortNum() throws IOException {
        nifiPropertiesTlsClientConfigWriter.getOverlayProperties().setProperty(nifiPropertiesTlsClientConfigWriter.getIncrementingPropertyMap().keySet().iterator().next(), "notAnInt");
        nifiPropertiesTlsClientConfigWriter.write(tlsClientConfig, outputStreamFactory);
    }

    @Test
    public void testNoHostnameProperties() throws IOException {
        nifiPropertiesTlsClientConfigWriter.getOverlayProperties().setProperty(NifiPropertiesTlsClientConfigWriter.HOSTNAME_PROPERTIES, "");
        nifiPropertiesTlsClientConfigWriter.write(tlsClientConfig, outputStreamFactory);
        testHostnamesAndPorts();
        Properties nifiProperties = getNifiProperties();
        nifiProperties.stringPropertyNames().forEach(s -> assertNotEquals(testHostname, nifiProperties.getProperty(s)));
    }

    private void testHostnamesAndPorts() {
        Properties nifiProperties = getNifiProperties();

        assertEquals(NifiPropertiesTlsClientConfigWriter.CONF + keyStore, nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE));
        assertEquals(keyStoreType, nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE));
        assertEquals(keyStorePassword, nifiProperties.getProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD));
        assertEquals(keyPassword, nifiProperties.getProperty(NiFiProperties.SECURITY_KEY_PASSWD));

        assertEquals(NifiPropertiesTlsClientConfigWriter.CONF + trustStore, nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE));
        assertEquals(trustStoreType, nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE));
        assertEquals(trustStorePassword, nifiProperties.getProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD));

        assertEquals("", nifiProperties.getProperty(NiFiProperties.WEB_HTTP_HOST));
        assertEquals("", nifiProperties.getProperty(NiFiProperties.WEB_HTTP_PORT));
        assertEquals(Boolean.toString(true), nifiProperties.getProperty(NiFiProperties.SITE_TO_SITE_SECURE));
        assertEquals(Boolean.toString(true), nifiProperties.getProperty(NiFiProperties.CLUSTER_PROTOCOL_IS_SECURE));

        nifiPropertiesTlsClientConfigWriter.getHostnamePropertyStream().forEach(s -> assertEquals(testHostname, nifiProperties.getProperty(s)));
        nifiPropertiesTlsClientConfigWriter.getIncrementingPropertyMap().entrySet().forEach(propertyToPortEntry -> {
            assertEquals(Integer.toString(propertyToPortEntry.getValue()), nifiProperties.getProperty(propertyToPortEntry.getKey()));
            assertEquals(Integer.parseInt(overlayProperties.getProperty(propertyToPortEntry.getKey())) + hostNum - 1, propertyToPortEntry.getValue().intValue());
        });
    }

    private Properties getNifiProperties() {
        Properties properties = new Properties();
        try {
            properties.load(new ByteArrayInputStream(outputStream.toByteArray()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }
}
