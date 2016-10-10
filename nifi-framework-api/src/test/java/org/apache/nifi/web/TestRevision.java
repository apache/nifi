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
package org.apache.nifi.web;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class TestRevision {

    @Test(expected = IllegalArgumentException.class)
    public void testNullVersion() throws Exception {
        new Revision(null, "client-id", "component-id");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullComponentId() throws Exception {
        new Revision(0l, "client-id", null);
    }

    @Test
    public void testIncrementRevision() throws Exception {
        final String clientId = "client-id";
        final String componentId = "component-id";
        final Revision revision = new Revision(0l, clientId, componentId);
        final Revision updatedRevision = revision.incrementRevision(clientId);
        assertEquals(1, updatedRevision.getVersion().longValue());
        assertEquals(clientId, updatedRevision.getClientId());
        assertEquals(componentId, updatedRevision.getComponentId());
    }

    @Test
    public void testIncrementRevisionNewClient() throws Exception {
        final String clientId = "client-id";
        final String newClientId = "new-client-id";
        final String componentId = "component-id";
        final Revision revision = new Revision(0l, clientId, componentId);
        final Revision updatedRevision = revision.incrementRevision(newClientId);
        assertEquals(1, updatedRevision.getVersion().longValue());
        assertEquals(newClientId, updatedRevision.getClientId());
        assertEquals(componentId, updatedRevision.getComponentId());
    }

}
