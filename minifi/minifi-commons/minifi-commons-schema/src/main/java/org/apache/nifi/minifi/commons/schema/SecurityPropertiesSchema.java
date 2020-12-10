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

package org.apache.nifi.minifi.commons.schema;

import org.apache.nifi.minifi.commons.schema.common.BaseSchema;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;
import org.apache.nifi.minifi.commons.schema.common.WritableSchema;

import java.util.Map;

import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SECURITY_PROPS_KEY;
import static org.apache.nifi.minifi.commons.schema.common.CommonPropertyKeys.SENSITIVE_PROPS_KEY;

/**
 *
 */
public class SecurityPropertiesSchema extends BaseSchema implements WritableSchema {

    public static final String KEYSTORE_KEY = "keystore";
    public static final String KEYSTORE_TYPE_KEY = "keystore type";
    public static final String KEYSTORE_PASSWORD_KEY = "keystore password";
    public static final String KEY_PASSWORD_KEY = "key password";
    public static final String TRUSTSTORE_KEY = "truststore";
    public static final String TRUSTSTORE_TYPE_KEY = "truststore type";
    public static final String TRUSTSTORE_PASSWORD_KEY = "truststore password";
    public static final String SSL_PROTOCOL_KEY = "ssl protocol";

    private String keystore = "";
    private String keystoreType = "";
    private String keystorePassword = "";
    private String keyPassword = "";
    private String truststore = "";
    private String truststoreType = "";
    private String truststorePassword = "";
    private String sslProtocol = "";
    private SensitivePropsSchema sensitiveProps;

    public SecurityPropertiesSchema() {
        sensitiveProps = new SensitivePropsSchema();
    }

    public SecurityPropertiesSchema(Map map) {
        keystore = getOptionalKeyAsType(map, KEYSTORE_KEY, String.class, SECURITY_PROPS_KEY, "");

        keystoreType = getOptionalKeyAsType(map, KEYSTORE_TYPE_KEY, String.class, SECURITY_PROPS_KEY, "");
        if (!StringUtil.isNullOrEmpty(keystoreType)) {
            if (validateStoreType(keystoreType)) {
                addValidationIssue(KEYSTORE_TYPE_KEY, SECURITY_PROPS_KEY, "it is not a supported type (must be either PKCS12 or JKS format)");
            }
        }

        keystorePassword = getOptionalKeyAsType(map, KEYSTORE_PASSWORD_KEY, String.class, SECURITY_PROPS_KEY, "");

        keyPassword = getOptionalKeyAsType(map, KEY_PASSWORD_KEY, String.class, SECURITY_PROPS_KEY, "");

        truststore = getOptionalKeyAsType(map, TRUSTSTORE_KEY, String.class, SECURITY_PROPS_KEY, "");

        truststoreType = getOptionalKeyAsType(map, TRUSTSTORE_TYPE_KEY, String.class, SECURITY_PROPS_KEY, "");
        if (!StringUtil.isNullOrEmpty(truststoreType)) {
            if (validateStoreType(truststoreType)) {
                addValidationIssue(TRUSTSTORE_TYPE_KEY, SECURITY_PROPS_KEY, "it is not a supported type (must be either PKCS12 or JKS format)");
            }
        }

        truststorePassword = getOptionalKeyAsType(map, TRUSTSTORE_PASSWORD_KEY, String.class, SECURITY_PROPS_KEY, "");

        sslProtocol = getOptionalKeyAsType(map, SSL_PROTOCOL_KEY, String.class, SECURITY_PROPS_KEY, "");
        if (!StringUtil.isNullOrEmpty(sslProtocol)) {
            switch (sslProtocol) {
                case "SSL":
                    break;
                case "SSLv2Hello":
                    break;
                case "SSLv3":
                    break;
                case "TLS":
                    break;
                case "TLSv1":
                    break;
                case "TLSv1.1":
                    break;
                case "TLSv1.2":
                    break;
                default:
                    addValidationIssue(SSL_PROTOCOL_KEY, SECURITY_PROPS_KEY, "it is not an allowable value of SSL protocol");
                    break;
            }
            if (StringUtil.isNullOrEmpty(keystore)) {
                addValidationIssue("When the '" + SSL_PROTOCOL_KEY + "' key of '" + SECURITY_PROPS_KEY + "' is set, the '" + KEYSTORE_KEY + "' must also be set");
            } else if (StringUtil.isNullOrEmpty(keystoreType) || StringUtil.isNullOrEmpty(keystorePassword) || StringUtil.isNullOrEmpty(keyPassword)) {
                addValidationIssue("When the '" + KEYSTORE_KEY + "' key of '" + SECURITY_PROPS_KEY + "' is set, the '" + KEYSTORE_TYPE_KEY + "', '" + KEYSTORE_PASSWORD_KEY +
                        "' and '" + KEY_PASSWORD_KEY + "' all must also be set");
            }

            if (!StringUtil.isNullOrEmpty(truststore) && (StringUtil.isNullOrEmpty(truststoreType) || StringUtil.isNullOrEmpty(truststorePassword))) {
                addValidationIssue("When the '" + TRUSTSTORE_KEY + "' key of '" + SECURITY_PROPS_KEY + "' is set, the '" + TRUSTSTORE_TYPE_KEY + "' and '" +
                        TRUSTSTORE_PASSWORD_KEY + "' must also be set");
            }
        }

        sensitiveProps = getMapAsType(map, SENSITIVE_PROPS_KEY, SensitivePropsSchema.class, SECURITY_PROPS_KEY, false);

        addIssuesIfNotNull(sensitiveProps);
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> result = mapSupplier.get();
        result.put(KEYSTORE_KEY, keystore);
        result.put(KEYSTORE_TYPE_KEY, keystoreType);
        result.put(KEYSTORE_PASSWORD_KEY, keystorePassword);
        result.put(KEY_PASSWORD_KEY, keyPassword);
        result.put(TRUSTSTORE_KEY, truststore);
        result.put(TRUSTSTORE_TYPE_KEY, truststoreType);
        result.put(TRUSTSTORE_PASSWORD_KEY, truststorePassword);
        result.put(SSL_PROTOCOL_KEY, sslProtocol);
        putIfNotNull(result, SENSITIVE_PROPS_KEY, sensitiveProps);
        return result;
    }

    private boolean validateStoreType(String store) {
        return !store.isEmpty() && !(store.equalsIgnoreCase("JKS") || store.equalsIgnoreCase("PKCS12"));
    }

    public boolean useSSL() {
        return !StringUtil.isNullOrEmpty(sslProtocol);
    }

    public String getKeystore() {
        return keystore;
    }

    public String getKeystoreType() {
        return keystoreType;
    }

    public String getKeystorePassword() {
        return keystorePassword;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public String getTruststore() {
        return truststore;
    }

    public String getTruststoreType() {
        return truststoreType;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

    public String getSslProtocol() {
        return sslProtocol;
    }

    public SensitivePropsSchema getSensitiveProps() {
        return sensitiveProps;
    }
}
