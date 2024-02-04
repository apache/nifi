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
package org.apache.nifi.snmp.factory.core;

import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.smi.UdpAddress;

public interface SNMPContext {

    default void setupTargetBasicProperties(final Target target, final SNMPConfiguration configuration) {
        final int snmpVersion = configuration.getVersion();
        final String host = configuration.getTargetHost();
        final String port = configuration.getTargetPort();
        final int retries = configuration.getRetries();
        final long timeout = configuration.getTimeoutInMs();

        target.setVersion(snmpVersion);
        target.setAddress(new UdpAddress(host + "/" + port));
        target.setRetries(retries);
        target.setTimeout(timeout);
    }

    Snmp createSnmpManagerInstance(final SNMPConfiguration configuration);

    Target createTargetInstance(final SNMPConfiguration configuration);
}
