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

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.snmp4j.Snmp;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import java.io.IOException;
import java.net.InetAddress;

public class SNMPManagerFactory {

    private static final String ALL_ADDRESSES = "0.0.0.0";

    public Snmp createSnmpManagerInstance(final SNMPConfiguration configuration) {
        final int port = configuration.getManagerPort();
        final Snmp snmpManager;
        try {
            final InetAddress listenAddress = InetAddress.getByName(ALL_ADDRESSES);
            final UdpAddress udpAddress = new UdpAddress(listenAddress, port);
            final DefaultUdpTransportMapping transportMapping = new DefaultUdpTransportMapping(udpAddress);
            snmpManager = new Snmp(transportMapping);
            snmpManager.listen();
        } catch (IOException e) {
            throw new ProcessException(e);
        }
        return snmpManager;
    }
}
