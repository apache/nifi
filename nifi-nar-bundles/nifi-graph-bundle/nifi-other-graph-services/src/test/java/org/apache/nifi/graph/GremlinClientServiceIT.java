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

package org.apache.nifi.graph;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * As of JanusGraph 0.3.X these tests can be a little inconsistent for a few runs at first.
 */
public class GremlinClientServiceIT {
    private TestRunner runner;
    private TestableGremlinClientService clientService;

    @Before
    public void setup() throws Exception {
        clientService = new TestableGremlinClientService();
        runner = TestRunners.newTestRunner(MockProcessor.class);
        runner.addControllerService("gremlinService", clientService);
        runner.setProperty(clientService, AbstractTinkerpopClientService.CONTACT_POINTS, "localhost");
        runner.setProperty(MockProcessor.GREMLIN_CLIENT, "gremlinService");
        runner.enableControllerService(clientService);
        runner.assertValid();

        String setup = IOUtils.toString(getClass().getResourceAsStream("/setup.gremlin"), "UTF-8");
        clientService.getClient().submit(setup);

        Assert.assertEquals("gremlin://localhost:8182/gremlin", clientService.getTransitUrl());
    }

    @After
    public void tearDown() throws Exception {
        String teardown = IOUtils.toString(getClass().getResourceAsStream("/teardown.gremlin"), "UTF-8");
        clientService.getClient().submit(teardown);
    }

    @Test
    public void testValueMap() {
        String gremlin = "g.V().hasLabel('dog').valueMap()";
        AtomicInteger integer = new AtomicInteger();
        Map<String, String> result = clientService.executeQuery(gremlin, new HashMap<>(), (record, isMore) -> integer.incrementAndGet());

        Assert.assertEquals(2, integer.get());
    }

    @Test
    public void testCount() {
        String gremlin = "g.V().hasLabel('dog').count()";
        AtomicInteger integer = new AtomicInteger();
        Map<String, String> result = clientService.executeQuery(gremlin, new HashMap<>(), (record, isMore) -> integer.incrementAndGet());
        Assert.assertEquals(1, integer.get());
    }
}
