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
package org.apache.nifi.cluster;

import org.apache.nifi.controller.cluster.ZooKeeperClientConfig;
import org.apache.nifi.properties.StandardNiFiProperties;
import org.apache.nifi.util.NiFiProperties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import java.util.Properties;

public class ZooKeeperClientConfigTest {

    private static final String LOCAL_CONNECT_STRING = "local:1234";
    private static final String STORE_TYPE = "JKS";
    private static final String ZOOKEEPER_KEYSTORE = "/zooKeeperKeystore.jks";
    private static final String ZOOKEEPER_TRUSTSTORE = "/zooKeeperTruststore.jks";

    @Test
    public void testEasyCase(){
        final String input = "local:1234";
        final String cleanedInput = ZooKeeperClientConfig.cleanConnectString(input);
        assertEquals(input, cleanedInput);
    }

    @Test
    public void testValidFunkyInput(){
        final String input = "local: 1234  ";
        final String cleanedInput = ZooKeeperClientConfig.cleanConnectString(input);
        assertEquals(LOCAL_CONNECT_STRING, cleanedInput);
    }

    @Test(expected = IllegalStateException.class)
    public void testInvalidSingleEntry(){
        ZooKeeperClientConfig.cleanConnectString("local: 1a34  ");
    }

    @Test
    public void testSingleEntryNoPort(){
        final String input = "local";
        final String cleanedInput = ZooKeeperClientConfig.cleanConnectString(input);
        assertEquals("local:2181", cleanedInput);
    }

    @Test
    public void testMultiValidEntry(){
        final String input = "local:1234,local:1235,local:1235,local:14952";
        final String cleanedInput = ZooKeeperClientConfig.cleanConnectString(input);
        assertEquals(input, cleanedInput);
    }

    @Test(expected = IllegalStateException.class)
    public void testMultiValidEntrySkipOne(){
        ZooKeeperClientConfig.cleanConnectString("local:1234,local:1235,local:12a5,local:14952");
    }

    @Test
    public void testMultiValidEntrySpacesForDays(){
        final String input = "   local   :   1234  , local:  1235,local  :1295,local:14952   ";
        final String cleanedInput = ZooKeeperClientConfig.cleanConnectString(input);
        assertEquals("local:1234,local:1235,local:1295,local:14952", cleanedInput);
    }

    @Test(expected = IllegalStateException.class)
    public void testMultiValidOneNonsense(){
        ZooKeeperClientConfig.cleanConnectString("   local   :   1234  , local:  1235:wack,local  :1295,local:14952   ");
    }

    @Test
    public void testValidClientSecureTrue() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, Boolean.TRUE.toString());

        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
        assertTrue(zkClientConfig.isClientSecure());
        assertEquals(zkClientConfig.getConnectionSocket(), ZooKeeperClientConfig.NETTY_CLIENT_CNXN_SOCKET);
    }

    @Test
    public void testValidClientSecureFalse() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, Boolean.FALSE.toString());

        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
        assertFalse(zkClientConfig.isClientSecure());
        assertEquals(zkClientConfig.getConnectionSocket(), ZooKeeperClientConfig.NIO_CLIENT_CNXN_SOCKET);
    }

    @Test
    public void testValidClientSecureEmpty() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "");

        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
        assertFalse(zkClientConfig.isClientSecure());
        assertEquals(zkClientConfig.getConnectionSocket(), ZooKeeperClientConfig.NIO_CLIENT_CNXN_SOCKET);
    }

    @Test
    public void testValidClientSecureSpaces() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, " true ");

        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
        assertTrue(zkClientConfig.isClientSecure());
        assertEquals(zkClientConfig.getConnectionSocket(), ZooKeeperClientConfig.NETTY_CLIENT_CNXN_SOCKET);
    }

    @Test
    public void testValidClientSecureUpperCase() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, Boolean.TRUE.toString());
        ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));

        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
        assertTrue(zkClientConfig.isClientSecure());
        assertEquals(zkClientConfig.getConnectionSocket(), ZooKeeperClientConfig.NETTY_CLIENT_CNXN_SOCKET);
    }

    @Test(expected = RuntimeException.class)
    public void testInvalidClientSecure() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.ZOOKEEPER_CLIENT_SECURE, "meh");
        ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
    }

    @Test
    public void testKeyStoreTypes() {
        final String storeType = "JKS";
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, storeType);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, storeType);

        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
        assertEquals(storeType, zkClientConfig.getKeyStoreType());
        assertEquals(storeType, zkClientConfig.getTrustStoreType());
    }

    @Test
    public void testKeyStoreTypesSpaces() {
        final String storeType = " JKS ";
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, storeType);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, storeType);

        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
        final String expectedStoreType = "JKS";
        assertEquals(expectedStoreType, zkClientConfig.getKeyStoreType());
        assertEquals(expectedStoreType, zkClientConfig.getTrustStoreType());
    }

    @Test
    public void testEmptyKeyStoreTypes() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, "");
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, "");

        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
        assertNull(zkClientConfig.getKeyStoreType());
        assertNull(zkClientConfig.getTrustStoreType());
    }

    @Test
    public void testBlankKeyStoreTypes() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, "    ");
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, "    ");

        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
        assertNull(zkClientConfig.getKeyStoreType());
        assertNull(zkClientConfig.getTrustStoreType());
    }

    @Test
    public void testValidCnxnSocketNames() {
        assertEquals("org.apache.zookeeper.ClientCnxnSocketNetty", ZooKeeperClientConfig.NETTY_CLIENT_CNXN_SOCKET);
        assertEquals("org.apache.zookeeper.ClientCnxnSocketNIO", ZooKeeperClientConfig.NIO_CLIENT_CNXN_SOCKET);
    }

    @Test
    public void testPreferredZookeeperTlsPropertyOnly() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE, "/zooKeeperKeystore.jks");
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, STORE_TYPE);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, "/zooKeeperTruststore.jks");
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, STORE_TYPE);

        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
        assertNotNull(zkClientConfig.getKeyStoreType());
        assertNotNull(zkClientConfig.getTrustStoreType());
    }

    @Test
    public void testPreferredStandardTlsPropertyOnly() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.SECURITY_KEYSTORE, "/standardKeystore.jks");
        properties.setProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE, STORE_TYPE);
        properties.setProperty(NiFiProperties.SECURITY_TRUSTSTORE, "/standardTruststore.jks");
        properties.setProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, STORE_TYPE);

        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
        assertNotNull(zkClientConfig.getKeyStoreType());
        assertNotNull(zkClientConfig.getTrustStoreType());
    }

    @Test
    public void testGetPreferredPropertyCombination() {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, LOCAL_CONNECT_STRING);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE, "/zooKeeperKeystore.jks");
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_KEYSTORE_TYPE, STORE_TYPE);
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE, "/zooKeeperTruststore.jks");
        properties.setProperty(NiFiProperties.ZOOKEEPER_SECURITY_TRUSTSTORE_TYPE, STORE_TYPE);
        properties.setProperty(NiFiProperties.SECURITY_KEYSTORE, "/standardKeystore.jks");
        properties.setProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE, STORE_TYPE);
        properties.setProperty(NiFiProperties.SECURITY_TRUSTSTORE, "/standardTruststore.jks");
        properties.setProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, STORE_TYPE);

        final ZooKeeperClientConfig zkClientConfig = ZooKeeperClientConfig.createConfig(new StandardNiFiProperties(properties));
        assertEquals(ZOOKEEPER_KEYSTORE, zkClientConfig.getKeyStore());
        assertEquals(ZOOKEEPER_TRUSTSTORE,zkClientConfig.getTrustStore());
    }
}
