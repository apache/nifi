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
package org.apache.nifi.snmp.operations;

import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.factory.CompositeSNMPFactory;
import org.apache.nifi.snmp.factory.SNMPFactory;
import org.snmp4j.Snmp;
import org.snmp4j.Target;

public final class SNMPRequestHandlerFactory {

    public static SNMPRequestHandler createStandardRequestHandler(final SNMPConfiguration configuration) {
        final SNMPFactory snmpFactory = new CompositeSNMPFactory();
        final Snmp snmpClient = snmpFactory.createSnmpManagerInstance(configuration);
        final Target target = snmpFactory.createTargetInstance(configuration);
        return new StandardSNMPRequestHandler(snmpClient, target);
    }

    private SNMPRequestHandlerFactory() {
        // This should not be instantiated.
    }

}
