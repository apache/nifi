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

import org.apache.nifi.snmp.configuration.SNMPConfiguration;
import org.apache.nifi.snmp.configuration.V2TrapConfiguration;
import org.apache.nifi.snmp.helper.TrapConfigurationFactory;
import org.apache.nifi.snmp.helper.configurations.SNMPConfigurationFactory;
import org.apache.nifi.snmp.helper.configurations.SNMPV1V2cConfigurationFactory;
import org.apache.nifi.snmp.processors.GetSNMP;
import org.apache.nifi.snmp.processors.ListenTrapSNMP;
import org.apache.nifi.snmp.processors.SendTrapSNMP;
import org.apache.nifi.snmp.processors.SetSNMP;
import org.apache.nifi.snmp.processors.properties.BasicProperties;
import org.apache.nifi.snmp.processors.properties.V2TrapProperties;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.snmp4j.mp.SnmpConstants;

public class SNMPV2cTestRunnerFactory implements SNMPTestRunnerFactory {

    private static final SNMPConfigurationFactory snmpV2cConfigurationFactory = new SNMPV1V2cConfigurationFactory(SnmpConstants.version2c);

    @Override
    public TestRunner createSnmpGetTestRunner(final int agentPort, final String oid, final String strategy) {
        final TestRunner runner = TestRunners.newTestRunner(GetSNMP.class);
        final SNMPConfiguration snmpConfiguration = snmpV2cConfigurationFactory.createSnmpGetSetConfiguration(agentPort);
        runner.setProperty(GetSNMP.OID, oid);
        runner.setProperty(GetSNMP.AGENT_HOST, snmpConfiguration.getTargetHost());
        runner.setProperty(GetSNMP.AGENT_PORT, snmpConfiguration.getTargetPort());
        runner.setProperty(BasicProperties.SNMP_COMMUNITY, snmpConfiguration.getCommunityString());
        runner.setProperty(BasicProperties.SNMP_VERSION, getVersionName(snmpConfiguration.getVersion()));
        runner.setProperty(GetSNMP.SNMP_STRATEGY, strategy);
        return runner;
    }

    @Override
    public TestRunner createSnmpSetTestRunner(int agentPort, final String oid, final String oidValue) {
        final TestRunner runner = TestRunners.newTestRunner(SetSNMP.class);
        final SNMPConfiguration snmpConfiguration = snmpV2cConfigurationFactory.createSnmpGetSetConfiguration(agentPort);
        runner.setProperty(SetSNMP.AGENT_HOST, snmpConfiguration.getTargetHost());
        runner.setProperty(SetSNMP.AGENT_PORT, snmpConfiguration.getTargetPort());
        runner.setProperty(BasicProperties.SNMP_COMMUNITY, snmpConfiguration.getCommunityString());
        runner.setProperty(BasicProperties.SNMP_VERSION, getVersionName(snmpConfiguration.getVersion()));
        final MockFlowFile flowFile = getFlowFile(oid, oidValue);
        runner.enqueue(flowFile);
        return runner;
    }

    @Override
    public TestRunner createSnmpSendTrapTestRunner(final int managerPort, final String oid, final String oidValue) {
        final TestRunner runner = TestRunners.newTestRunner(SendTrapSNMP.class);
        final SNMPConfiguration snmpConfiguration = snmpV2cConfigurationFactory.createSnmpGetSetConfiguration(managerPort);
        final V2TrapConfiguration trapConfiguration = TrapConfigurationFactory.getV2TrapConfiguration();
        runner.setProperty(SendTrapSNMP.SNMP_MANAGER_HOST, snmpConfiguration.getTargetHost());
        runner.setProperty(SendTrapSNMP.SNMP_MANAGER_PORT, snmpConfiguration.getTargetPort());
        runner.setProperty(BasicProperties.SNMP_COMMUNITY, snmpConfiguration.getCommunityString());
        runner.setProperty(BasicProperties.SNMP_VERSION, getVersionName(snmpConfiguration.getVersion()));
        runner.setProperty(V2TrapProperties.TRAP_OID_VALUE, trapConfiguration.getTrapOidValue());
        final MockFlowFile flowFile = getFlowFile(oid, oidValue);
        runner.enqueue(flowFile);
        return runner;
    }

    @Override
    public TestRunner createSnmpListenTrapTestRunner(final int managerPort) {
        final TestRunner runner = TestRunners.newTestRunner(ListenTrapSNMP.class);
        final SNMPConfiguration snmpConfiguration = snmpV2cConfigurationFactory.createSnmpListenTrapConfig(managerPort);
        runner.setProperty(ListenTrapSNMP.SNMP_MANAGER_PORT, String.valueOf(snmpConfiguration.getManagerPort()));
        runner.setProperty(BasicProperties.SNMP_COMMUNITY, snmpConfiguration.getCommunityString());
        runner.setProperty(BasicProperties.SNMP_VERSION, getVersionName(snmpConfiguration.getVersion()));
        return runner;
    }
}
