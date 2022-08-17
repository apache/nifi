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

package org.apache.nifi.minifi.bootstrap;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public enum SensitiveProperty {
    SECRET_KEY("secret.key"),
    C2_SECURITY_TRUSTSTORE_PASSWORD("c2.security.truststore.password"),
    C2_SECURITY_KEYSTORE_PASSWORD("c2.security.keystore.password"),
    NIFI_MINIFI_SECURITY_KEYSTORE_PASSWORD("nifi.minifi.security.keystorePasswd"),
    NIFI_MINIFI_SECURITY_TRUSTSTORE_PASSWORD("nifi.minifi.security.truststorePasswd"),
    NIFI_MINIFI_SENSITIVE_PROPS_KEY("nifi.minifi.sensitive.props.key");

    public static final Set<String> SENSITIVE_PROPERTIES = Arrays.stream(SensitiveProperty.values()).map(SensitiveProperty::getKey)
        .collect(collectingAndThen(toSet(), Collections::unmodifiableSet));

    private final String key;

    SensitiveProperty(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }
}
