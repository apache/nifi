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

import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestPutEmail {

    @Test
    public void testHotNotFound() {
        // verifies that files are routed to failure when the SMTP host doesn't exist
        final TestRunner runner = TestRunners.newTestRunner(new PutEmail());
        runner.setProperty(PutEmail.SMTP_HOSTNAME, "host-doesnt-exist123");
        runner.setProperty(PutEmail.FROM, "test@apache.org");
        runner.setProperty(PutEmail.TO, "test@apache.org");
        runner.setProperty(PutEmail.MESSAGE, "Message Body");

        final Map<String, String> attributes = new HashMap<>();
        runner.enqueue("Some Text".getBytes(), attributes);

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(PutEmail.REL_FAILURE);
    }
}
