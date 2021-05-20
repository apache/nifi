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
package org.apache.nifi.snmp.helper.testrunners;

import org.apache.nifi.snmp.exception.InvalidSnmpVersionException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.snmp4j.mp.SnmpConstants;

import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.snmp.utils.SNMPUtils.SNMP_PROP_PREFIX;

public interface SNMPTestRunnerFactory {

    TestRunner createSnmpGetTestRunner(final int agentPort, final String oid, final String strategy);

    TestRunner createSnmpSetTestRunner(final int agentPort, final String oid, final String oidValue);

    TestRunner createSnmpSendTrapTestRunner(final int managerPort, final String oid, final String oidValue);

    TestRunner createSnmpListenTrapTestRunner(final int managerPort);

    default MockFlowFile getFlowFile(String oid, String oidValue) {
        final MockFlowFile flowFile = new MockFlowFile(1L);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(SNMP_PROP_PREFIX + oid, oidValue);
        flowFile.putAttributes(attributes);
        return flowFile;
    }

    default String getVersionName(final int version) {
        if (version == SnmpConstants.version1) {
            return "SNMPv1";
        } else if (version == SnmpConstants.version2c) {
            return "SNMPv2c";
        } else if (version == SnmpConstants.version3) {
            return "SNMPv3";
        } else {
            throw new InvalidSnmpVersionException("Invalid SNMP version");
        }
    }
}
