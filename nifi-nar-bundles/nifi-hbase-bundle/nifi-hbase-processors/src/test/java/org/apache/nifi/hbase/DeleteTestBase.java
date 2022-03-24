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

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DeleteTestBase {
    protected TestRunner runner;
    protected MockHBaseClientService hBaseClient;

    public void setup(Class clz) throws InitializationException {
        runner = TestRunners.newTestRunner(clz);

        hBaseClient = new MockHBaseClientService();
        runner.addControllerService("hbaseClient", hBaseClient);
        runner.enableControllerService(hBaseClient);

        runner.setProperty(DeleteHBaseRow.TABLE_NAME, "nifi");
        runner.setProperty(DeleteHBaseRow.HBASE_CLIENT_SERVICE, "hbaseClient");
    }

    List<String> populateTable(int max) {
        List<String> ids = new ArrayList<>();
        for (int index = 0; index < max; index++) {
            String uuid = UUID.randomUUID().toString();
            ids.add(uuid);
            Map<String, String> cells = new HashMap<>();
            cells.put("test", UUID.randomUUID().toString());
            hBaseClient.addResult(uuid, cells, System.currentTimeMillis());
        }

        return ids;
    }
}
