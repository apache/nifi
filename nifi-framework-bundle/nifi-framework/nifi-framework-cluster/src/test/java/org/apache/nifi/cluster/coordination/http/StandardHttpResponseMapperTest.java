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
package org.apache.nifi.cluster.coordination.http;

import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardHttpResponseMapperTest {
    private static final String CONNECTOR_ID = "abcdef00-abcd-abcd-abcd-abcdef000000";
    private static final String REQUEST_ID = "00000000-0000-0000-0000-000000000001";

    private final StandardHttpResponseMapper mapper = new StandardHttpResponseMapper(NiFiProperties.createBasicNiFiProperties((String) null));

    @Test
    void testConnectorPurgeRequestsInterpreted() {
        assertTrue(mapper.isResponseInterpreted(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/purge-requests"), "POST"));
        assertTrue(mapper.isResponseInterpreted(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/purge-requests/" + REQUEST_ID), "GET"));
        assertTrue(mapper.isResponseInterpreted(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/purge-requests/" + REQUEST_ID), "DELETE"));
    }

    @Test
    void testUnsupportedConnectorPurgeRequestsNotInterpreted() {
        assertFalse(mapper.isResponseInterpreted(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/purge-requests"), "GET"));
    }
}
