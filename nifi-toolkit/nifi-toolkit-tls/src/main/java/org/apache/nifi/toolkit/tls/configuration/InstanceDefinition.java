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

import org.apache.nifi.toolkit.tls.standalone.TlsToolkitStandaloneCommandLine;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Contains an instance identifier, a number that either corresponds to the instance identifier or to the global order and relevant passwords
 */
public class InstanceDefinition {
    private final InstanceIdentifier instanceIdentifier;
    private final int number;
    private final String keyStorePassword;
    private final String keyPassword;
    private final String trustStorePassword;

    public InstanceDefinition(InstanceIdentifier instanceIdentifier, int number, String keyStorePassword, String keyPassword, String trustStorePassword) {
        this.number = number;
        this.instanceIdentifier = instanceIdentifier;
        this.keyStorePassword = keyStorePassword;
        this.keyPassword = keyPassword;
        this.trustStorePassword = trustStorePassword;
    }

    /**
     * Creates a list of instance definitions
     *
     * @param fullHostNameExpressions    the host expressions defining the global order (null if not relevant)
     * @param currentHostnameExpressions the host expressions to create instance definitions for
     * @param keyStorePasswords          a supplier for keyStorePasswords
     * @param keyPasswords               a supplier for keyPasswords
     * @param trustStorePasswords        a supplier for trustStorePasswords
     * @return a list of instance definitions
     */
    public static List<InstanceDefinition> createDefinitions(Stream<String> fullHostNameExpressions, Stream<String> currentHostnameExpressions, Supplier<String> keyStorePasswords,
                                                             Supplier<String> keyPasswords, Supplier<String> trustStorePasswords) {
        if (fullHostNameExpressions == null) {
            return InstanceIdentifier.createIdentifiers(currentHostnameExpressions).map(id -> createDefinition(id, id.getNumber(), keyStorePasswords, keyPasswords, trustStorePasswords))
                    .collect(Collectors.toList());
        } else {
            Map<InstanceIdentifier, Integer> orderMap = InstanceIdentifier.createOrderMap(fullHostNameExpressions);
            return InstanceIdentifier.createIdentifiers(currentHostnameExpressions).map(id -> {
                Integer number = orderMap.get(id);
                if (number == null) {
                    throw new IllegalArgumentException("Unable to find " + id.getHostname() + " in specified " + TlsToolkitStandaloneCommandLine.GLOBAL_PORT_SEQUENCE_ARG + " expression(s).");
                }
                return createDefinition(id, number, keyStorePasswords, keyPasswords, trustStorePasswords);
            }).collect(Collectors.toList());
        }
    }

    protected static InstanceDefinition createDefinition(InstanceIdentifier instanceIdentifier, int number, Supplier<String> keyStorePasswords, Supplier<String> keyPasswords,
                                                         Supplier<String> trustStorePasswords) {
        String keyStorePassword = keyStorePasswords.get();
        String keyPassword;
        if (keyPasswords == null) {
            keyPassword = keyStorePassword;
        } else {
            keyPassword = keyPasswords.get();
        }
        String trustStorePassword = trustStorePasswords.get();
        return new InstanceDefinition(instanceIdentifier, number, keyStorePassword, keyPassword, trustStorePassword);
    }

    public String getHostname() {
        return instanceIdentifier.getHostname();
    }

    public int getNumber() {
        return number;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public InstanceIdentifier getInstanceIdentifier() {
        return instanceIdentifier;
    }
}
