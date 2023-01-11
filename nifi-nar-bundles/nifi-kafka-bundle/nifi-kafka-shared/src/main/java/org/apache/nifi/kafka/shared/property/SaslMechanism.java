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
package org.apache.nifi.kafka.shared.property;

import org.apache.nifi.components.DescribedValue;

import java.util.Arrays;
import java.util.Optional;

/**
 * Enumeration of supported Kafka SASL Mechanisms
 */
public enum SaslMechanism implements DescribedValue {
    GSSAPI("GSSAPI", "GSSAPI", "General Security Services API for Kerberos authentication"),

    PLAIN("PLAIN", "PLAIN", "Plain username and password authentication"),

    SCRAM_SHA_256("SCRAM-SHA-256", "SCRAM-SHA-256", "Salted Challenge Response Authentication Mechanism using SHA-512 with username and password"),

    SCRAM_SHA_512("SCRAM-SHA-512", "SCRAM-SHA-512", "Salted Challenge Response Authentication Mechanism using SHA-256 with username and password");

    private final String value;

    private final String displayName;

    private final String description;

    SaslMechanism(final String value, final String displayName, final String description) {
        this.value = value;
        this.displayName = displayName;
        this.description = description;
    }

    public static SaslMechanism getSaslMechanism(final String value) {
        final Optional<SaslMechanism> foundSaslMechanism = Arrays.stream(SaslMechanism.values())
                .filter(saslMechanism -> saslMechanism.getValue().equals(value))
                .findFirst();
        return foundSaslMechanism.orElseThrow(() -> new IllegalArgumentException(String.format("SaslMechanism value [%s] not found", value)));
    }

    @Override
    public String getValue() {
        return value;
    }

    @Override
    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String getDescription() {
        return description;
    }
}
