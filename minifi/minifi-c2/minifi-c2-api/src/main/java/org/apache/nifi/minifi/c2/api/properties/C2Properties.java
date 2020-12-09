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

package org.apache.nifi.minifi.c2.api.properties;

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Properties;

public class C2Properties extends Properties {
    public static final String MINIFI_C2_SERVER_SECURE = "minifi.c2.server.secure";
    public static final String MINIFI_C2_SERVER_KEYSTORE_TYPE = "minifi.c2.server.keystoreType";
    public static final String MINIFI_C2_SERVER_KEYSTORE = "minifi.c2.server.keystore";
    public static final String MINIFI_C2_SERVER_KEYSTORE_PASSWD = "minifi.c2.server.keystorePasswd";
    public static final String MINIFI_C2_SERVER_KEY_PASSWD = "minifi.c2.server.keyPasswd";
    public static final String MINIFI_C2_SERVER_TRUSTSTORE = "minifi.c2.server.truststore";
    public static final String MINIFI_C2_SERVER_TRUSTSTORE_TYPE = "minifi.c2.server.truststoreType";
    public static final String MINIFI_C2_SERVER_TRUSTSTORE_PASSWD = "minifi.c2.server.truststorePasswd";

    private static final Logger logger = LoggerFactory.getLogger(C2Properties.class);
    private static final C2Properties properties = initProperties();
    private static final String C2_SERVER_HOME = System.getenv("C2_SERVER_HOME");

    private static C2Properties initProperties() {
        C2Properties properties = new C2Properties();
        try (InputStream inputStream = C2Properties.class.getClassLoader().getResourceAsStream("c2.properties")) {
            properties.load(inputStream);
        } catch (IOException e) {
            throw new RuntimeException("Unable to load c2.properties", e);
        }
        return properties;
    }

    public static C2Properties getInstance() {
        return properties;
    }

    public boolean isSecure() {
        return Boolean.valueOf(getProperty(MINIFI_C2_SERVER_SECURE, "false"));
    }

    public SslContextFactory getSslContextFactory() throws GeneralSecurityException, IOException {
        SslContextFactory sslContextFactory = new SslContextFactory();
        KeyStore keyStore = KeyStore.getInstance(properties.getProperty(MINIFI_C2_SERVER_KEYSTORE_TYPE));
        Path keyStorePath = Paths.get(C2_SERVER_HOME).resolve(properties.getProperty(MINIFI_C2_SERVER_KEYSTORE)).toAbsolutePath();
        logger.debug("keystore path: " + keyStorePath);
        try (InputStream inputStream = Files.newInputStream(keyStorePath)) {
            keyStore.load(inputStream, properties.getProperty(MINIFI_C2_SERVER_KEYSTORE_PASSWD).toCharArray());
        }
        sslContextFactory.setKeyStore(keyStore);
        sslContextFactory.setKeyManagerPassword(properties.getProperty(MINIFI_C2_SERVER_KEY_PASSWD));
        sslContextFactory.setWantClientAuth(true);

        String trustStorePath = Paths.get(C2_SERVER_HOME).resolve(properties.getProperty(MINIFI_C2_SERVER_TRUSTSTORE)).toAbsolutePath().toFile().getAbsolutePath();
        logger.debug("truststore path: " + trustStorePath);
        sslContextFactory.setTrustStorePath(trustStorePath);
        sslContextFactory.setTrustStoreType(properties.getProperty(MINIFI_C2_SERVER_TRUSTSTORE_TYPE));
        sslContextFactory.setTrustStorePassword(properties.getProperty(MINIFI_C2_SERVER_TRUSTSTORE_PASSWD));
        try {
            sslContextFactory.start();
        } catch (Exception e) {
            throw new IOException(e);
        }
        return sslContextFactory;
    }
}
