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
package org.apache.nifi.integration.accesscontrol;

import com.sun.jersey.api.client.ClientResponse;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Access control test for funnels.
 */
public class ITCountersAccessControl {

    private static AccessControlHelper helper;
    private static String uri;

    @BeforeClass
    public static void setup() throws Exception {
        helper = new AccessControlHelper();
        uri = helper.getBaseUrl() + "/counters";
    }

    /**
     * Test get counters.
     *
     * @throws Exception exception
     */
    @Test
    public void testGetCounters() throws Exception {
        helper.testGenericGetUri(uri);
    }

    /**
     * Ensures the READ user can get counters.
     *
     * @throws Exception ex
     */
    @Test
    public void testUpdateCounters() throws Exception {
        final String counterUri = uri + "/my-counter";

        ClientResponse response;

        // read
        response = helper.getReadUser().testPut(counterUri, Collections.emptyMap());
        assertEquals(403, response.getStatus());

        // read/write
        response = helper.getReadWriteUser().testPut(counterUri, Collections.emptyMap());
        assertEquals(404, response.getStatus());

        // write
        response = helper.getWriteUser().testPut(counterUri, Collections.emptyMap());
        assertEquals(404, response.getStatus());

        // none
        response = helper.getNoneUser().testPut(counterUri, Collections.emptyMap());
        assertEquals(403, response.getStatus());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        helper.cleanup();
    }
}
