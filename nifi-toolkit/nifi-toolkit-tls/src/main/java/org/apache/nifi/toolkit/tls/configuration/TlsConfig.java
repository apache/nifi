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

package org.apache.nifi.toolkit.tls.configuration;

import org.apache.nifi.toolkit.tls.util.TlsHelper;
import org.apache.nifi.util.StringUtils;

import java.security.NoSuchAlgorithmException;

public class TlsConfig {
    public static final String DEFAULT_HOSTNAME = "localhost";
    public static final String DEFAULT_KEY_STORE_TYPE = "jks";
    public static final int DEFAULT_PORT = 8443;

    private TlsHelperConfig tlsHelperConfig;
    private String keyStore;
    private String keyStoreType = DEFAULT_KEY_STORE_TYPE;
    private String keyStorePassword;
    private String keyPassword;
    private String token;
    private String caHostname = DEFAULT_HOSTNAME;
    private int port = DEFAULT_PORT;

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public TlsHelperConfig getTlsHelperConfig() {
        return tlsHelperConfig;
    }

    public void setTlsHelperConfig(TlsHelperConfig tlsHelperConfig) {
        this.tlsHelperConfig = tlsHelperConfig;
    }

    public String getKeyStore() {
        return keyStore;
    }

    public void setKeyStore(String keyStore) {
        this.keyStore = keyStore;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getCaHostname() {
        return caHostname;
    }

    public void setCaHostname(String caHostname) {
        this.caHostname = caHostname;
    }

    public TlsHelper createTlsHelper() throws NoSuchAlgorithmException {
        return new TlsHelper(getTlsHelperConfig());
    }

    public void initDefaults() {
        if (port == 0) {
            port = DEFAULT_PORT;
        }
        if (StringUtils.isEmpty(keyStoreType)) {
            keyStoreType = DEFAULT_KEY_STORE_TYPE;
        }
        if (StringUtils.isEmpty(caHostname)) {
            caHostname = DEFAULT_HOSTNAME;
        }
        if (tlsHelperConfig == null) {
            tlsHelperConfig = new TlsHelperConfig();
        }
        tlsHelperConfig.initDefaults();
    }
}
