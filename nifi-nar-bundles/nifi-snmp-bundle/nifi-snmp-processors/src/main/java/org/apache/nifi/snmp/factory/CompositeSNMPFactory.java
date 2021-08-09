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
package org.apache.nifi.snmp.factory;

import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.exception.InvalidSnmpVersionException;
import org.snmp4j.Snmp;
import org.snmp4j.Target;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class CompositeSNMPFactory implements SNMPFactory {

    private static final String INVALID_SNMP_VERSION = "SNMP version is not supported.";
    private static final List<SNMPFactory> FACTORIES;

    static {
        final List<SNMPFactory> factories = Arrays.asList(new V1SNMPFactory(), new V2cSNMPFactory(), new V3SNMPFactory());
        FACTORIES = Collections.unmodifiableList(factories);
    }

    @Override
    public boolean supports(final int version) {
        return !getMatchingFactory(version).isPresent();
    }

    @Override
    public Snmp createSnmpManagerInstance(final SNMPConfiguration configuration) {
        final Optional<SNMPFactory> factory = getMatchingFactory(configuration.getVersion());
        if (!factory.isPresent()) {
            throw new InvalidSnmpVersionException(INVALID_SNMP_VERSION);
        }
        return factory.get().createSnmpManagerInstance(configuration);
    }

    @Override
    public Target createTargetInstance(final SNMPConfiguration configuration) {
        final Optional<SNMPFactory> factory = getMatchingFactory(configuration.getVersion());
        if (!factory.isPresent()) {
            throw new InvalidSnmpVersionException(INVALID_SNMP_VERSION);
        }
        return factory.get().createTargetInstance(configuration);
    }

    private Optional<SNMPFactory> getMatchingFactory(final int version) {
        for (final SNMPFactory factory : FACTORIES) {
            if (factory.supports(version)) {
                return Optional.of(factory);
            }
        }
        return Optional.empty();
    }
}
