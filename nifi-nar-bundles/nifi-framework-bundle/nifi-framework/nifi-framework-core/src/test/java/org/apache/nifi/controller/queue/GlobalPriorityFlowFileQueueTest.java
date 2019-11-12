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

package org.apache.nifi.controller.queue;

import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.priority.Rule;
import org.apache.nifi.priority.RulesManager;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.controller.queue.GlobalPriorityFlowFileQueue.FLOW_FILE_RULE_CACHE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GlobalPriorityFlowFileQueueTest {
    private GlobalPriorityFlowFileQueue globalPriorityFlowFileQueue;

    @Before
    public void before() {
        globalPriorityFlowFileQueue = new GlobalPriorityFlowFileQueue("id", null, null, null, null, new RulesManager(null, null), 5, 1000L, "0 B", null, 100, null);
    }

    @Test
    // In this test we'll load the connection up with expired flowfiles and verify that as soon as an expired flowfile is encountered
    // that poll continues to burn off expired records even if they are of a lower priority until it hits the limit.
    public void testExpiredLimit() {
        // Expire everything quickly
        globalPriorityFlowFileQueue.setFlowFileExpiration("1ms");

        Map<String, String> highPriority = new HashMap<>(1);
        highPriority.put("priority", "0");
        FlowFileRecord highPriorityRecord = new MockFlowFileRecord(highPriority, 0L);
        Map<String, String> mediumPriority = new HashMap<>(1);
        mediumPriority.put("priority", "1");

        globalPriorityFlowFileQueue.put(highPriorityRecord);
        List<FlowFileRecord> flowFileList = new ArrayList<>(GlobalPriorityFlowFileQueue.MAX_EXPIRED_RECORDS);
        for(int i = 0; i < GlobalPriorityFlowFileQueue.MAX_EXPIRED_RECORDS; i++) {
            FlowFileRecord flowFileRecord = new MockFlowFileRecord(mediumPriority, 0L);
            flowFileList.add(flowFileRecord);
        }
        globalPriorityFlowFileQueue.putAll(flowFileList);

        // Sanity check, ensure we have GlobalPriorityFlowFileQueue.MAX_EXPIRED_RECORDS + 1 files in the connection
        assertEquals(GlobalPriorityFlowFileQueue.MAX_EXPIRED_RECORDS + 1, globalPriorityFlowFileQueue.size().getObjectCount());
        Set<FlowFileRecord> expiredFlowFileSet = new HashSet<>(GlobalPriorityFlowFileQueue.MAX_EXPIRED_RECORDS);

        // We do not have any unexpired flowfiles in the connection
        assertNull(globalPriorityFlowFileQueue.poll(expiredFlowFileSet));

        // There were GlobalPriorityFlowFileQueue.MAX_EXPIRED_RECORDS expired flowfiles when we polled (the limit)
        assertEquals(GlobalPriorityFlowFileQueue.MAX_EXPIRED_RECORDS, expiredFlowFileSet.size());
        expiredFlowFileSet.clear();

        // 1 should be left after we burned off the max expired
        assertEquals(1, globalPriorityFlowFileQueue.size().getObjectCount());
    }

    @Test
    // Verify that when there's an expired record ahead of an unexpired record, the unexpired one is returned and the expired
    // one is added to the appropriate set
    public void testExpired() throws InterruptedException {
        Set<FlowFileRecord> expiredFlowFileSet = new HashSet<>(1);
        globalPriorityFlowFileQueue.setFlowFileExpiration("500ms");

        Map<String,String> highPriority = new HashMap<>(1);
        highPriority.put("priority", "0");
        FlowFileRecord expiredRecord = new MockFlowFileRecord(highPriority, 0L);

        globalPriorityFlowFileQueue.put(expiredRecord);

        Thread.sleep(globalPriorityFlowFileQueue.getFlowFileExpiration(TimeUnit.MILLISECONDS) + 1);

        FlowFileRecord unexpiredRecord = new MockFlowFileRecord(highPriority, 0L);
        globalPriorityFlowFileQueue.put(unexpiredRecord);

        assertEquals(unexpiredRecord, globalPriorityFlowFileQueue.poll(expiredFlowFileSet));
        assertEquals(1, expiredFlowFileSet.size());
        assertEquals(0, globalPriorityFlowFileQueue.size().getObjectCount());
    }

    @Test
    public void testPut() throws ExecutionException {
        MockFlowFileRecord flowFileNotAssociatedWithRule = new MockFlowFileRecord(0L);
        MockFlowFileRecord flowFileForExpiredRule = new MockFlowFileRecord(1L);
        MockFlowFileRecord flowFileForUnexpiredRule1 = new MockFlowFileRecord(2L);
        MockFlowFileRecord flowFileForUnexpiredRule2 = new MockFlowFileRecord(3L);

        Rule expiredRule = Rule.builder().expression("${x:equals('expired')}").expired(true).label("expired").build();
        FLOW_FILE_RULE_CACHE.get().put(flowFileForExpiredRule, expiredRule);
        Rule unexpiredRule = Rule.builder().expression("${x:equals('unexpired')}").label("unexpired").build();
        FLOW_FILE_RULE_CACHE.get().put(flowFileForUnexpiredRule1, unexpiredRule);
        FLOW_FILE_RULE_CACHE.get().put(flowFileForUnexpiredRule2, unexpiredRule);

        // Case 1: Rule has yet to be evaluated and alwaysReevaluate is false so should map to UNEVALUATED
        globalPriorityFlowFileQueue.put(flowFileNotAssociatedWithRule);
        assert(FLOW_FILE_RULE_CACHE.get().asMap().containsKey(flowFileNotAssociatedWithRule));
        assertEquals(Rule.UNEVALUATED, FLOW_FILE_RULE_CACHE.get().get(flowFileNotAssociatedWithRule));
        assertEquals(1, globalPriorityFlowFileQueue.size().getObjectCount());

        // Case 2: Rule exists and is not expired. Rule should not change in the map
        globalPriorityFlowFileQueue.put(flowFileForUnexpiredRule1);
        assertEquals(unexpiredRule, FLOW_FILE_RULE_CACHE.get().get(flowFileForUnexpiredRule1));
        assertEquals(2, globalPriorityFlowFileQueue.size().getObjectCount());

        // Case 3: Rule exists and is expired. Map should be updated with new appropriate rule (DEFAULT in this case)
        globalPriorityFlowFileQueue.put(flowFileForExpiredRule);
        assertEquals(Rule.DEFAULT, FLOW_FILE_RULE_CACHE.get().get(flowFileForExpiredRule));
        assertEquals(3, globalPriorityFlowFileQueue.size().getObjectCount());

        // Case 4: Rule exists and is not expired but alwaysReevaluate is true
        globalPriorityFlowFileQueue.alwaysReevaluate = true;
        globalPriorityFlowFileQueue.put(flowFileForUnexpiredRule2);
        // Even though this file was already in the global static map, the rule manager is unaware of it so when its reevaluated
        // you should get the DEFAULT rule in return since there's no priority attribute on this file.
        assertEquals(Rule.DEFAULT, FLOW_FILE_RULE_CACHE.get().get(flowFileForUnexpiredRule2));
        assertEquals(4, globalPriorityFlowFileQueue.size().getObjectCount());
    }
}