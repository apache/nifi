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
package org.apache.nifi.cluster.coordination.http.endpoints;

import org.junit.Test;

import java.net.URI;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AccessPolicyEndpointMergerTest {

    @Test
    public void testCanHandle() throws Exception {
        final AccessPolicyEndpointMerger merger = new AccessPolicyEndpointMerger();
        assertTrue(merger.canHandle(URI.create("http://localhost:8080/nifi-api/policies"), "POST"));
        assertFalse(merger.canHandle(URI.create("http://localhost:8080/nifi-api/policies"), "GET"));
        assertFalse(merger.canHandle(URI.create("http://localhost:8080/nifi-api/policies"), "PUT"));
        assertTrue(merger.canHandle(URI.create("http://localhost:8080/nifi-api/policies/" + UUID.randomUUID().toString()), "PUT"));
        assertFalse(merger.canHandle(URI.create("http://localhost:8080/nifi-api/policies/Read/flow"), "GET"));
        assertTrue(merger.canHandle(URI.create("http://localhost:8080/nifi-api/policies/read/flow"), "GET"));
        assertTrue(merger.canHandle(URI.create("http://localhost:8080/nifi-api/policies/read/processors/" + UUID.randomUUID().toString()), "GET"));
    }

}