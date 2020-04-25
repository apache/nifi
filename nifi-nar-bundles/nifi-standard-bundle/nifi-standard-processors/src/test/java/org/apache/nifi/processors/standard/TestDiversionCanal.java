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
package org.apache.nifi.processors.standard;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestDiversionCanal {

    @Test
    public void testRouteToPrimary() {
        final TestRunner runner = TestRunners.newTestRunner(new DiversionCanal());
        runner.enqueue(new byte[0]);

        runner.run();

        runner.assertAllFlowFilesTransferred(DiversionCanal.REL_PRIMARY, 1);
    }

    @Test
    public void testRouteToSecondary() {
        final TestRunner runner = TestRunners.newTestRunner(new DiversionCanal());
        runner.enqueue(new byte[0]);

        runner.setRelationshipUnavailable(DiversionCanal.REL_PRIMARY);
        runner.run();

        runner.assertAllFlowFilesTransferred(DiversionCanal.REL_SECONDARY, 1);
    }

    @Test
    public void testBothUnavailable() {
        final TestRunner runner = TestRunners.newTestRunner(new DiversionCanal());
        runner.enqueue(new byte[0]);

        runner.setRelationshipUnavailable(DiversionCanal.REL_PRIMARY);
        runner.setRelationshipUnavailable(DiversionCanal.REL_SECONDARY);
        runner.run();

        runner.assertTransferCount(DiversionCanal.REL_PRIMARY, 0);
        runner.assertTransferCount(DiversionCanal.REL_SECONDARY, 0);
    }
}
