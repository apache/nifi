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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.snmp4j.AbstractTarget;
import org.snmp4j.CommunityTarget;
import org.snmp4j.Snmp;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OID;

/**
 * Class to test SNMP Get processor
 */
public class WalkSNMPTest {

    /** agent for test with SNMP v1 */
    private static TestSnmpAgentV1 agentv1 = null;

    /** Root OID to perform a WALK */
    private static final OID root = new OID("1.3.6.1.2.1.1");

    /**
     * Method to set up different SNMP agents
     * @throws Exception Exception
     */
    @BeforeClass
    public static void setUp() throws Exception {
        agentv1 = new TestSnmpAgentV1("0.0.0.0/2002");
        agentv1.start();
    }

    /**
     * Method to close SNMP Agent once the tests are completed
     * @throws Exception Exception
     */
    @AfterClass
    public static void tearDown() throws Exception {
        agentv1.stop();
    }

    /**
     * Method to test successful SNMP Walk in case of v1
     * @throws Exception Exception
     */
    @Test
    public void validateSuccessfullSnmpWalkVersion1() throws Exception {
        Snmp snmp = SNMPUtilsTest.createSnmp();
        CommunityTarget target = SNMPUtilsTest.createCommTarget("public", "127.0.0.1/2002", SnmpConstants.version1);

        GetSNMP pubProc = new LocalGetSnmp(snmp, target);
        TestRunner runner = TestRunners.newTestRunner(pubProc);

        runner.setProperty(GetSNMP.OID, root.toString());
        runner.setProperty(GetSNMP.SNMP_STRATEGY, "WALK");
        runner.setProperty(GetSNMP.HOST, "127.0.0.1");
        runner.setProperty(GetSNMP.PORT, "2002");
        runner.setProperty(GetSNMP.SNMP_COMMUNITY, "public");
        runner.setProperty(GetSNMP.SNMP_VERSION, "SNMPv1");

        runner.run();
        Thread.sleep(200);
        final MockFlowFile successFF = runner.getFlowFilesForRelationship(GetSNMP.REL_SUCCESS).get(0);
        assertNotNull(successFF);

        int i = 0;
        for (String attributeKey : successFF.getAttributes().keySet()) {
            if(attributeKey.startsWith(SNMPUtils.SNMP_PROP_PREFIX)) {
                i++;
            }
        }
        assertEquals(8,i);

    }

    /**
     * Local extension of SNMP Getter
     */
    private class LocalGetSnmp extends GetSNMP {
        /** SNMP */
        private Snmp snmp;
        /** Target to request */
        private AbstractTarget target;
        /**
         * Constructor
         * @param snmp SNMP
         * @param target Target
         */
        public LocalGetSnmp(Snmp snmp, AbstractTarget target) {
            this.snmp = snmp;
            this.target = target;
        }
    }

}
