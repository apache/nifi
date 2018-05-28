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

package org.apache.nifi.hbase;

import org.apache.nifi.hbase.util.VisibilityUtil;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

public class TestVisibilityUtil {
    private TestRunner runner;

    @Before
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(PutHBaseCell.class);
        final MockHBaseClientService hBaseClient = new MockHBaseClientService();
        runner.addControllerService("hbaseClient", hBaseClient);
        runner.enableControllerService(hBaseClient);
        runner.setProperty(PutHBaseCell.HBASE_CLIENT_SERVICE, "hbaseClient");
        runner.setProperty(PutHBaseCell.TABLE_NAME, "test");
        runner.setProperty(PutHBaseCell.COLUMN_QUALIFIER, "test");
        runner.setProperty(PutHBaseCell.COLUMN_FAMILY, "test");
        runner.assertValid();
    }

    @Test
    public void testAllPresentOnFlowfile() {
        runner.setProperty("visibility.test.test", "U&PII");

        MockFlowFile ff = new MockFlowFile(System.currentTimeMillis());
        ff.putAttributes(new HashMap<String, String>(){{
            put("visibility.test.test", "U&PII&PHI");
        }});
        ProcessContext context = runner.getProcessContext();

        String label = VisibilityUtil.pickVisibilityString("test", "test", ff, context);

        Assert.assertNotNull(label);
        Assert.assertEquals("U&PII&PHI", label);
    }

    @Test
    public void testOnlyColumnFamilyOnFlowfile() {
        runner.setProperty("visibility.test", "U&PII");

        MockFlowFile ff = new MockFlowFile(System.currentTimeMillis());
        ff.putAttributes(new HashMap<String, String>(){{
            put("visibility.test", "U&PII&PHI");
        }});
        ProcessContext context = runner.getProcessContext();

        String label = VisibilityUtil.pickVisibilityString("test", "test", ff, context);

        Assert.assertNotNull(label);
        Assert.assertEquals("U&PII&PHI", label);
    }

    @Test
    public void testInvalidAttributes() {
        runner.setProperty("visibility.test", "U&PII");

        MockFlowFile ff = new MockFlowFile(System.currentTimeMillis());
        ff.putAttributes(new HashMap<String, String>(){{
            put("visibility..test", "U&PII&PHI");
        }});
        ProcessContext context = runner.getProcessContext();

        String label = VisibilityUtil.pickVisibilityString("test", "test", ff, context);

        Assert.assertNotNull(label);
        Assert.assertEquals("U&PII", label);
    }

    @Test
    public void testColumnFamilyAttributeOnly() {
        MockFlowFile ff = new MockFlowFile(System.currentTimeMillis());
        ff.putAttributes(new HashMap<String, String>(){{
            put("visibility.test", "U&PII");
        }});
        ProcessContext context = runner.getProcessContext();

        String label = VisibilityUtil.pickVisibilityString("test", "test", ff, context);

        Assert.assertNotNull(label);
        Assert.assertEquals("U&PII", label);
    }

    @Test
    public void testNoAttributes() {
        runner.setProperty("visibility.test", "U&PII");

        MockFlowFile ff = new MockFlowFile(System.currentTimeMillis());
        ProcessContext context = runner.getProcessContext();

        String label = VisibilityUtil.pickVisibilityString("test", "test", ff, context);

        Assert.assertNotNull(label);
        Assert.assertEquals("U&PII", label);

        runner.setProperty("visibility.test.test", "U&PII&PHI");
        label = VisibilityUtil.pickVisibilityString("test", "test", ff, context);

        Assert.assertNotNull(label);
        Assert.assertEquals("U&PII&PHI", label);

    }
}