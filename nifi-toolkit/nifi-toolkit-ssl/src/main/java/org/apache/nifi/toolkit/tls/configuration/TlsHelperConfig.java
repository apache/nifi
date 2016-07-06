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

public class TlsHelperConfig {
    public static final String NIFI_TOOLKIT_TLS_HELPER_DAYS = "nifi.toolkit.tls.helper.days";
    public static final String NIFI_TOOLKIT_TLS_HELPER_KEY_SIZE = "nifi.toolkit.tls.helper.keySize";
    public static final String NIFI_TOOLKIT_TLS_HELPER_KEY_PAIR_ALGORITHM = "nifi.toolkit.tls.helper.keyPairAlgorithm";
    public static final String NIFI_TOOLKIT_TLS_HELPER_SIGNING_ALGORITHM = "nifi.toolkit.tls.helper.signingAlgorithm";
    public static final String NIFI_TOOLKIT_TLS_HELPER_KEY_STORE_TYPE = "nifi.toolkit.tls.helper.keyStoreType";
    public static final int DEFAULT_DAYS = 365;
    public static final int DEFAULT_KEY_SIZE = 2048;
    public static final String DEFAULT_KEY_PAIR_ALGORITHM = "RSA";
    public static final String DEFAULT_SIGNING_ALGORITHM = "SHA256WITHRSA";
    public static final String DEFAULT_KEY_STORE_TYPE = "jks";

    private int days = DEFAULT_DAYS;
    private int keySize = DEFAULT_KEY_SIZE;
    private String keyPairAlgorithm = DEFAULT_KEY_PAIR_ALGORITHM;
    private String signingAlgorithm = DEFAULT_SIGNING_ALGORITHM;
    private String keyStoreType = DEFAULT_KEY_STORE_TYPE;

    public TlsHelperConfig() {
    }

    public TlsHelperConfig(int days, int keySize, String keyPairAlgorithm, String signingAlgorithm, String keyStoreType) {
        this.days = days;
        this.keySize = keySize;
        this.keyPairAlgorithm = keyPairAlgorithm;
        this.signingAlgorithm = signingAlgorithm;
        this.keyStoreType = keyStoreType;
    }

    public int getDays() {
        return days;
    }

    public void setDays(int days) {
        this.days = days;
    }

    public int getKeySize() {
        return keySize;
    }

    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }

    public String getKeyPairAlgorithm() {
        return keyPairAlgorithm;
    }

    public void setKeyPairAlgorithm(String keyPairAlgorithm) {
        this.keyPairAlgorithm = keyPairAlgorithm;
    }

    public String getSigningAlgorithm() {
        return signingAlgorithm;
    }

    public void setSigningAlgorithm(String signingAlgorithm) {
        this.signingAlgorithm = signingAlgorithm;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }
}
