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
package org.apache.nifi.snmp.processors;

import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.snmp.helper.testrunners.SNMPV1TestRunnerFactory;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunner;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class GetSNMPTest {

    private static final String OID = "1.3.6.1.4.1.32437.1.5.1.4.2.0";

    @Test
    public void testOnTriggerWithGetStrategyPerformsSnmpGet() {
        final TestRunner getSnmpTestRunner = new SNMPV1TestRunnerFactory().createSnmpGetTestRunner(NetworkUtils.getAvailableUdpPort(), OID, "GET");
        final GetSNMP spyGetSNMP = spy((GetSNMP) getSnmpTestRunner.getProcessor());
        final MockProcessSession mockProcessSession = new MockProcessSession(new SharedSessionState(spyGetSNMP, new AtomicLong(0L)), spyGetSNMP);

        doNothing().when(spyGetSNMP).performSnmpGet(any(), any(), any(), any());

        spyGetSNMP.onTrigger(getSnmpTestRunner.getProcessContext(), mockProcessSession);

        verify(spyGetSNMP).performSnmpGet(any(), any(), any(), any());
    }

    @Test
    public void testOnTriggerWithWalkStrategyPerformsSnmpWalk() {
        final TestRunner getSnmpTestRunner = new SNMPV1TestRunnerFactory().createSnmpGetTestRunner(NetworkUtils.getAvailableUdpPort(), OID, "WALK");
        final GetSNMP spyGetSNMP = spy((GetSNMP) getSnmpTestRunner.getProcessor());
        final MockProcessSession mockProcessSession = new MockProcessSession(new SharedSessionState(spyGetSNMP, new AtomicLong(0L)), spyGetSNMP);

        doNothing().when(spyGetSNMP).performSnmpWalk(any(), any(), any(), any());

        spyGetSNMP.onTrigger(getSnmpTestRunner.getProcessContext(), mockProcessSession);

        verify(spyGetSNMP).performSnmpWalk(any(), any(), any(), any());
    }
}
