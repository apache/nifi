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
package org.apache.nifi.snmp.helper;

import org.apache.nifi.snmp.configuration.V1TrapConfiguration;
import org.apache.nifi.snmp.configuration.V2TrapConfiguration;
import org.snmp4j.PDUv1;

public class TrapConfigurationFactory {

    // v1 specific
    private static final String ENTERPRISE_OID = "1.3.5.7.11";
    private static final String AGENT_ADDRESS = "1.2.3.4";
    private static final String GENERIC_TRAP_TYPE = String.valueOf(PDUv1.ENTERPRISE_SPECIFIC);
    private static final String SPECIFIC_TRAP_TYPE = "2";

    // v2c/v3 specific
    private static final String TRAP_OID_VALUE = "testTrapOidValue";

    public static V1TrapConfiguration getV1TrapConfiguration() {
        return V1TrapConfiguration.builder()
                .enterpriseOid(ENTERPRISE_OID)
                .agentAddress(AGENT_ADDRESS)
                .genericTrapType(GENERIC_TRAP_TYPE)
                .specificTrapType(SPECIFIC_TRAP_TYPE)
                .build();
    }

    public static V2TrapConfiguration getV2TrapConfiguration() {
        return new V2TrapConfiguration(TRAP_OID_VALUE);
    }
}
